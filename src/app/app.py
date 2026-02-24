# src/app/app.py
from __future__ import annotations

import os
import pandas as pd
import streamlit as st

from src.rag.retrieve import retrieve_schema_chunks
from src.t2sql.prompt_builder import build_prompt
from src.t2sql.guardrails import validate_and_fix
from src.t2sql.generate import call_ollama, extract_sql
from src.t2sql.executor import run_sql

DOMAINS = ["chinook", "dvdrental", "northwind"]


@st.cache_resource
def get_settings():
    return {
        "ollama_url": os.getenv("OLLAMA_URL", "http://localhost:11434"),
        "ollama_model": os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b"),
        "persist_dir": os.getenv("CHROMA_DIR", "data/chroma"),
        "limit": int(os.getenv("SQL_LIMIT", "200")),
    }


@st.cache_data(show_spinner=False)
def cached_retrieve(domain: str, question: str, k: int, persist_dir: str):
    return retrieve_schema_chunks(domain=domain, question=question, k=k, persist_dir=persist_dir)


def main():
    st.set_page_config(page_title="Multi-Domain NL→SQL Copilot", layout="wide")

    cfg = get_settings()

    st.title("🧠 Multi-Domain NL→SQL Copilot (RAG + Ollama + Postgres)")
    st.caption("Retrieve schema → Generate SQL → Run → Results + CSV")

    with st.sidebar:
        st.header("Settings")
        domain = st.selectbox("Domain (DB)", DOMAINS, index=0)
        k = st.slider("Top-K schema chunks", 3, 20, 15)

        st.text_input("Ollama URL", value=cfg["ollama_url"], key="ollama_url")
        st.text_input("Ollama Model", value=cfg["ollama_model"], key="ollama_model")
        st.text_input("Chroma persist dir", value=cfg["persist_dir"], key="persist_dir")

        limit = st.number_input(
            "Row LIMIT", min_value=50, max_value=1000, value=cfg["limit"], step=50
        )

    col1, col2 = st.columns([2, 1])
    with col1:
        question = st.text_area("Ask a question:", value="top 5 customers by spend", height=90)

        b1, b2, b3 = st.columns(3)
        do_retrieve = b1.button("🔎 Retrieve", use_container_width=True)
        do_generate = b2.button("✨ Generate SQL", use_container_width=True)
        do_run = b3.button("▶ Run SQL", use_container_width=True)

    with col2:
        st.subheader("Status")
        st.write(f"**Domain:** `{domain}`")
        st.write(f"**Top-K:** `{k}`")
        st.write(f"**Model:** `{st.session_state['ollama_model']}`")

    # session state
    st.session_state.setdefault("chunks", [])
    st.session_state.setdefault("sql", "")
    st.session_state.setdefault("results_df", None)
    st.session_state.setdefault("error", "")

    def show_error(e):
        st.session_state["error"] = str(e)
        st.error(st.session_state["error"])

    # Actions
    if do_retrieve:
        try:
            st.session_state["error"] = ""
            st.session_state["chunks"] = cached_retrieve(
                domain, question, k, st.session_state["persist_dir"]
            )
            st.success(f"Retrieved {len(st.session_state['chunks'])} schema chunks.")
        except Exception as e:
            show_error(e)

    if do_generate:
        try:
            st.session_state["error"] = ""
            if not st.session_state["chunks"]:
                st.session_state["chunks"] = cached_retrieve(
                    domain, question, k, st.session_state["persist_dir"]
                )

            prompt = build_prompt(
                domain=domain,
                question=question,
                chunks=st.session_state["chunks"],
                dialect="PostgreSQL",
            )
            raw = call_ollama(
                prompt=prompt,
                model=st.session_state["ollama_model"],
                base_url=st.session_state["ollama_url"],
            )
            sql = validate_and_fix(extract_sql(raw), limit=int(limit))
            st.session_state["sql"] = sql
            st.success("SQL generated + validated (SELECT-only + LIMIT enforced).")
        except Exception as e:
            show_error(e)

    if do_run:
        try:
            st.session_state["error"] = ""
            if not st.session_state["sql"]:
                # generate first if needed
                if not st.session_state["chunks"]:
                    st.session_state["chunks"] = cached_retrieve(
                        domain, question, k, st.session_state["persist_dir"]
                    )
                prompt = build_prompt(
                    domain=domain,
                    question=question,
                    chunks=st.session_state["chunks"],
                    dialect="PostgreSQL",
                )
                raw = call_ollama(
                    prompt=prompt,
                    model=st.session_state["ollama_model"],
                    base_url=st.session_state["ollama_url"],
                )
                st.session_state["sql"] = validate_and_fix(extract_sql(raw), limit=int(limit))

            cols, rows = run_sql(dbname=domain, sql=st.session_state["sql"], max_rows=int(limit))
            st.session_state["results_df"] = pd.DataFrame(rows, columns=cols)
            st.success(f"Query executed. Rows returned: {len(st.session_state['results_df'])}")
        except Exception as e:
            show_error(e)

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
            st.info("Click **Retrieve** to view schema context used for generation.")

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
