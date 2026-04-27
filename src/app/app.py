# src/app/app.py
from __future__ import annotations

import json
import os
import random
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import streamlit as st

from src.rag.retrieve import retrieve_schema_chunks
from src.t2sql.prompt_builder import build_prompt
from src.t2sql.guardrails import validate_and_fix
from src.t2sql.generate import call_ollama, call_openai, call_groq, call_hf_inference, extract_sql
from src.t2sql.executor import run_sql

DOMAINS = ["nyc_311", "olist_ecommerce", "synthea_patients"]

DOMAIN_LABELS = {
    "nyc_311":          "🏙️ NYC 311 — Government Complaints",
    "olist_ecommerce":  "🛒 Olist — Brazilian E-Commerce",
    "synthea_patients": "🏥 Synthea — Healthcare Patients",
}

DOMAIN_DESCRIPTIONS = {
    "nyc_311":          "1 table · 300k+ service requests · boroughs, agencies, complaint types",
    "olist_ecommerce":  "8 tables · 100k+ orders · sellers, products, reviews, payments",
    "synthea_patients": "6 tables · 10k synthetic patients · encounters, conditions, medications",
}


# ── HF Spaces: auto-build chroma index if missing ────────────────────────────
def _ensure_chroma_index() -> None:
    """On HF Spaces (USE_SQLITE=true), build the chroma index on first boot."""
    chroma_dir = os.getenv("CHROMA_DIR", "data/chroma")
    if os.path.isdir(chroma_dir) and any(os.scandir(chroma_dir)):
        return  # already built
    try:
        import subprocess
        with st.spinner("⏳ Building schema index (first boot — takes ~60s)..."):
            result = subprocess.run(
                [sys.executable, "scripts/02_build_index.py", "--all"],
                capture_output=True, text=True, timeout=300,
            )
        if result.returncode != 0:
            st.error(f"Index build failed:\n{result.stderr[:500]}")
    except Exception as e:
        st.error(f"Index build error: {e}")


def _ensure_olist_db() -> None:
    """
    On HF Spaces: try to build olist_ecommerce.db at runtime using Kaggle credentials.
    Silently skips if KAGGLE_USERNAME / KAGGLE_KEY secrets are not set.
    """
    olist_path = Path("data/sqlite/olist_ecommerce.db")
    if olist_path.exists() and olist_path.stat().st_size > 1_000_000:
        return  # already built
    api_token = os.getenv("KAGGLE_API_TOKEN")          # new single-token style (KGAT_...)
    username  = os.getenv("KAGGLE_USERNAME")
    key       = os.getenv("KAGGLE_KEY") or os.getenv("KAGGLE_API_KEY")
    if not api_token and not (username and key):
        return  # no credentials — skip silently
    try:
        import subprocess
        with st.spinner("⏳ Downloading Olist database (first boot — ~3 min)..."):
            result = subprocess.run(
                [sys.executable, "scripts/build_databases.py", "--only", "olist"],
                capture_output=True, text=True, timeout=600,
            )
        if result.returncode != 0:
            st.warning(f"Olist DB build failed:\n{result.stderr[:300]}")
    except Exception as e:
        st.warning(f"Olist DB build error: {e}")


def _available_domains() -> list[str]:
    """Return domains whose SQLite databases actually exist (used in HF mode)."""
    if not _HF_MODE:
        return DOMAINS
    db_dir = Path("data/sqlite")
    db_map = {
        "nyc_311":          "nyc_311.db",
        "olist_ecommerce":  "olist_ecommerce.db",
        "synthea_patients": "synthea_patients.db",
    }
    return [d for d in DOMAINS if (db_dir / db_map[d]).exists()]


# ── Mode detection ────────────────────────────────────────────────────────────
# USE_SQLITE=true  → SQLite mode (HF Spaces / no Postgres)
# Backend priority: Groq (free) → OpenAI → Ollama (local)
_USE_SQLITE = os.getenv("USE_SQLITE", "").lower() in ("1", "true", "yes")
_HF_MODE = _USE_SQLITE

def _default_backend() -> str:
    if os.getenv("GROQ_API_KEY"):
        return "groq"
    if os.getenv("HF_TOKEN"):
        return "hf"
    if os.getenv("OPENAI_API_KEY"):
        return "openai"
    return "ollama"


@st.cache_resource
def get_settings():
    return {
        "ollama_url":   os.getenv("OLLAMA_URL",   "http://localhost:11434"),
        "ollama_model": os.getenv("OLLAMA_MODEL",  "qwen2.5-coder:7b"),
        "openai_model": os.getenv("OPENAI_MODEL",  "gpt-4o-mini"),
        "groq_model":   os.getenv("GROQ_MODEL",    "llama-3.3-70b-versatile"),
        "hf_model":     os.getenv("HF_MODEL",      "Qwen/Qwen2.5-Coder-7B-Instruct"),
        "persist_dir":  os.getenv("CHROMA_DIR",    "data/chroma"),
        "limit":        int(os.getenv("SQL_MAX_ROWS", "200")),
        "openai_key":   os.getenv("OPENAI_API_KEY", ""),
        "groq_key":     os.getenv("GROQ_API_KEY", ""),
        "hf_token":     os.getenv("HF_TOKEN", ""),
    }


@st.cache_data(show_spinner=False)
def cached_retrieve(domain: str, question: str, k: int, persist_dir: str):
    return retrieve_schema_chunks(domain=domain, question=question, k=k, persist_dir=persist_dir)


def llm_generate(prompt: str, cfg: dict, sidebar_openai_key: str, sidebar_groq_key: str, sidebar_hf_token: str) -> str:
    """Route to HF / Groq / OpenAI / Ollama based on sidebar selection."""
    backend = st.session_state.get("backend", _default_backend())
    if backend == "hf":
        api_key = sidebar_hf_token or cfg["hf_token"]
        return call_hf_inference(prompt, model=cfg["hf_model"], api_key=api_key)
    elif backend == "groq":
        api_key = sidebar_groq_key or cfg["groq_key"]
        return call_groq(prompt, model=cfg["groq_model"], api_key=api_key)
    elif backend == "openai":
        api_key = sidebar_openai_key or cfg["openai_key"]
        return call_openai(prompt, model=cfg["openai_model"], api_key=api_key)
    else:
        return call_ollama(prompt, model=st.session_state["ollama_model"],
                           base_url=st.session_state["ollama_url"])


def main():
    st.set_page_config(page_title="NL→SQL Copilot", layout="wide", page_icon="🧠")

    if _HF_MODE:
        _ensure_olist_db()      # try Kaggle if KAGGLE_KEY secret is set
        _ensure_chroma_index()  # rebuild chroma if missing

    cfg = get_settings()

    st.title("🧠 Multi-Domain NL→SQL Copilot")
    if _HF_MODE:
        st.caption("RAG + OpenAI + SQLite — running on Hugging Face Spaces")
        st.info(
            "**Demo mode:** Using real-world SQLite databases (NYC 311, Olist E-Commerce, Synthea Healthcare). "
            "Enter your API key in the sidebar to generate SQL.",
            icon="ℹ️",
        )
    else:
        st.caption("RAG + LLM + PostgreSQL — retrieve schema → generate SQL → run → results")

    # ── Sidebar ────────────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("⚙️ Settings")

        # Domain selector — only show databases that are actually available
        avail_domains = _available_domains()
        domain = st.selectbox(
            "Domain (database)",
            avail_domains,
            index=0,
            format_func=lambda d: DOMAIN_LABELS.get(d, d),
        )
        st.caption(DOMAIN_DESCRIPTIONS.get(domain, ""))
        if _HF_MODE and len(avail_domains) < len(DOMAINS):
            missing = [DOMAIN_LABELS[d] for d in DOMAINS if d not in avail_domains]
            st.caption(f"⚠️ Unavailable: {', '.join(missing)} — add KAGGLE_API_TOKEN Secret to enable.")

        k = st.slider("Top-K schema chunks", 3, 20, 15)

        st.divider()
        st.subheader("🤖 LLM Backend")

        backend_options = ["groq", "openai", "ollama", "hf"] if not _HF_MODE else ["groq", "openai", "hf"]
        backend_labels = {
            "groq":   "⚡ Groq — llama-3.3-70b (free, fast)",
            "openai": "🤖 OpenAI — gpt-4o-mini",
            "ollama": "🖥️ Ollama — local model",
            "hf":     "🤗 HuggingFace Inference API",
        }
        default_idx = backend_options.index(_default_backend()) if _default_backend() in backend_options else 0
        backend = st.selectbox(
            "LLM Backend",
            backend_options,
            index=default_idx,
            key="backend",
            format_func=lambda b: backend_labels.get(b, b),
            help="Groq is free. OpenAI requires an API key with credits. Ollama runs locally.",
        )

        groq_key_input = ""
        openai_key_input = ""
        sidebar_hf_token = ""

        if backend == "hf":
            sidebar_hf_token = st.text_input(
                "HF Token",
                type="password",
                value="",
                placeholder="hf_..." if not cfg["hf_token"] else "set via env ✓",
                help="Free at huggingface.co/settings/tokens (Read token is enough).",
            )
            st.caption(f"Model: `{cfg['hf_model']}`")
            st.caption("⭐ Similar class to the 96.6% benchmark model")
        elif backend == "groq":
            groq_key_input = st.text_input(
                "Groq API Key",
                type="password",
                value="",
                placeholder="gsk_..." if not cfg["groq_key"] else "set via env ✓",
                help="Free at console.groq.com — stored only in session memory.",
            )
            st.caption(f"Model: `{cfg['groq_model']}`")
        elif backend == "openai":
            openai_key_input = st.text_input(
                "OpenAI API Key",
                type="password",
                value="",
                placeholder="sk-..." if not cfg["openai_key"] else "set via env ✓",
                help="Stored only in session memory — never persisted.",
            )
            st.caption(f"Model: `{cfg['openai_model']}`")
        else:
            st.text_input("Ollama URL",   value=cfg["ollama_url"],   key="ollama_url")
            st.text_input("Ollama Model", value=cfg["ollama_model"], key="ollama_model")

        st.divider()
        st.text_input("Chroma persist dir", value=cfg["persist_dir"], key="persist_dir")
        limit = st.number_input("Row LIMIT", min_value=50, max_value=1000, value=cfg["limit"], step=50)

        if _HF_MODE:
            st.divider()
            st.markdown(
                "**Benchmark results** (59 gold queries):\n\n"
                "| Model | Accuracy |\n"
                "|---|---|\n"
                "| OpenAI gpt-4o-mini | **96.6%** |\n"
                "| Groq llama-3.3-70b-versatile | **96.6%** |\n"
                "| Groq llama-3.1-8b-instant | 94.9% |\n"
                "| Ollama llama3.2:3b (local) | 67.8% |\n\n"
                "_Datasets: NYC 311 · Olist E-Commerce · Synthea Healthcare_"
            )

    # ── Example questions loader ───────────────────────────────────────────────
    _demo_path = "eval/demo_questions.json"
    _demo_questions: dict = {}
    try:
        with open(_demo_path, encoding="utf-8") as _f:
            _demo_questions = json.load(_f)
    except Exception:
        pass

    # Reset example when domain changes
    if st.session_state.get("_last_domain") != domain:
        st.session_state["_last_domain"] = domain
        st.session_state["_example_q"] = ""

    # ── Main area ─────────────────────────────────────────────────────────────
    col1, col2 = st.columns([2, 1])
    with col1:
        default_q = (
            st.session_state.get("_example_q")
            or (_demo_questions.get(domain, [""])[0] if _demo_questions.get(domain) else
                "What are the top 5 most common complaint types across all boroughs?")
        )
        question = st.text_area(
            "Ask a question about the database:",
            value=default_q,
            height=90,
            key=f"question_{domain}",
        )
        b1, b2, b3, b4 = st.columns(4)
        do_retrieve = b1.button("🔎 Retrieve Schema", use_container_width=True)
        do_generate = b2.button("✨ Generate SQL",    use_container_width=True)
        do_run      = b3.button("▶ Run SQL",          use_container_width=True)
        if b4.button("🎲 Example",               use_container_width=True):
            examples = _demo_questions.get(domain, [])
            if examples:
                st.session_state["_example_q"] = random.choice(examples)
                st.rerun()

    with col2:
        st.subheader("Status")
        st.write(f"**Domain:** `{domain}`")
        st.write(f"**Backend:** `{st.session_state.get('backend', 'openai')}`")
        st.write(f"**DB mode:** `{'SQLite' if _HF_MODE else 'PostgreSQL'}`")
        st.write(f"**Top-K:** `{k}`")

    # ── Session state init ─────────────────────────────────────────────────────
    st.session_state.setdefault("chunks", [])
    st.session_state.setdefault("sql", "")
    st.session_state.setdefault("results_df", None)
    st.session_state.setdefault("error", "")

    def show_error(e: Exception):
        st.session_state["error"] = str(e)
        st.error(str(e))

    # ── Action: Retrieve ──────────────────────────────────────────────────────
    if do_retrieve:
        try:
            st.session_state["error"] = ""
            st.session_state["chunks"] = cached_retrieve(
                domain, question, k, st.session_state["persist_dir"]
            )
            st.success(f"Retrieved {len(st.session_state['chunks'])} schema chunks.")
        except Exception as e:
            show_error(e)

    # ── Action: Generate SQL ──────────────────────────────────────────────────
    if do_generate:
        try:
            st.session_state["error"] = ""
            if not st.session_state["chunks"]:
                st.session_state["chunks"] = cached_retrieve(
                    domain, question, k, st.session_state["persist_dir"]
                )
            # Use SQLite dialect on Spaces (syntax is nearly identical to Postgres for SELECTs)
            dialect = "SQLite" if _HF_MODE else "PostgreSQL"
            prompt = build_prompt(domain=domain, question=question,
                                  chunks=st.session_state["chunks"], dialect=dialect)
            with st.spinner("Generating SQL..."):
                raw = llm_generate(prompt, cfg, openai_key_input, groq_key_input, sidebar_hf_token)
            sql = validate_and_fix(extract_sql(raw), limit=int(limit))
            st.session_state["sql"] = sql
            st.success("SQL generated ✓  (SELECT-only + LIMIT enforced)")
        except Exception as e:
            show_error(e)

    # ── Action: Run SQL ────────────────────────────────────────────────────────
    if do_run:
        try:
            st.session_state["error"] = ""
            if not st.session_state["sql"]:
                # auto-generate first
                if not st.session_state["chunks"]:
                    st.session_state["chunks"] = cached_retrieve(
                        domain, question, k, st.session_state["persist_dir"]
                    )
                dialect = "SQLite" if _HF_MODE else "PostgreSQL"
                prompt = build_prompt(domain=domain, question=question,
                                      chunks=st.session_state["chunks"], dialect=dialect)
                with st.spinner("Generating SQL..."):
                    raw = llm_generate(prompt, cfg, openai_key_input, groq_key_input, sidebar_hf_token)
                st.session_state["sql"] = validate_and_fix(extract_sql(raw), limit=int(limit))

            with st.spinner("Running query..."):
                cols, rows = run_sql(dbname=domain, sql=st.session_state["sql"], max_rows=int(limit))
            st.session_state["results_df"] = pd.DataFrame(rows, columns=cols)
            st.success(f"Query executed. Rows returned: {len(st.session_state['results_df'])}")
        except Exception as e:
            show_error(e)

    # ── Results layout ────────────────────────────────────────────────────────
    st.divider()
    left, right = st.columns([1, 1])

    with left:
        st.subheader("📚 Retrieved Schema Chunks")
        if st.session_state["chunks"]:
            for i, c in enumerate(st.session_state["chunks"], start=1):
                meta = c.get("meta", {}) or {}
                dist = c.get("distance", None)
                dist_str = f"{dist:.4f}" if isinstance(dist, (int, float)) else "n/a"
                with st.expander(f"Chunk {i} | table={meta.get('table')} | dist={dist_str}"):
                    st.code(c["text"])
        else:
            st.info("Click **Retrieve Schema** to view schema context used for generation.")

    with right:
        st.subheader("🧾 Generated SQL")
        if st.session_state["sql"]:
            st.code(st.session_state["sql"], language="sql")
        else:
            st.info("Click **Generate SQL** to produce a query.")

        st.subheader("📊 Results")
        df = st.session_state["results_df"]
        if df is not None:
            st.dataframe(df, use_container_width=True, height=320)
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇ Download CSV",
                data=csv,
                file_name=f"{domain}_results.csv",
                mime="text/csv",
            )
        else:
            st.info("Click **Run SQL** to execute the query and see results.")


if __name__ == "__main__":
    main()
