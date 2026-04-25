# eval/evaluate_compare.py
"""
Multi-model benchmark: qwen2.5-coder:7b (Ollama, local) vs gpt-5.4-mini (OpenAI).

Runs all gold queries against both models, tracks:
  - Execution accuracy
  - Latency per query (seconds)
  - Token usage + estimated cost (OpenAI only; Ollama = $0)

Outputs:
  - eval/compare_report.csv   (per-query detail for both models)
  - eval/compare_summary.csv  (side-by-side model summary)
  - Printed summary table

Usage:
    python -m eval.evaluate_compare                         # both models
    python -m eval.evaluate_compare --models ollama         # local only
    python -m eval.evaluate_compare --models gpt-5.4-mini   # OpenAI only
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

from src.rag.retrieve import retrieve_schema_chunks
from src.t2sql.prompt_builder import build_prompt
from src.t2sql.generate import call_ollama, call_groq, extract_sql
from src.t2sql.guardrails import validate_and_fix
from src.t2sql.executor import run_sql
from eval.eval_utils import normalize_df, same_result, estimate_cost

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
GOLD_PATH = Path("eval/gold.jsonl")
TOP_K = 15
LIMIT = 200
ROUND_DECIMALS = 2

# Pricing constants live in eval_utils — imported above


# ---------------------------------------------------------------------------
# OpenAI call
# ---------------------------------------------------------------------------
def call_gpt(
    prompt: str,
    model: str = "gpt-5.4-mini",
    api_key: Optional[str] = None,
) -> Tuple[str, int, int]:
    """
    Call OpenAI chat completion.
    Returns (sql_text, input_tokens, output_tokens).
    """
    try:
        from openai import OpenAI
    except ImportError:
        raise ImportError("openai package not installed. Run: pip install openai>=1.0.0")

    key = api_key or os.getenv("OPENAI_API_KEY")
    if not key:
        raise ValueError(
            "OPENAI_API_KEY not set. Add it to your .env file:\n"
            "  OPENAI_API_KEY=sk-..."
        )

    client = OpenAI(api_key=key)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a precise Text-to-SQL generator. Output ONLY SQL."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.1,
        max_completion_tokens=512,
    )
    text = response.choices[0].message.content or ""
    input_tokens = response.usage.prompt_tokens
    output_tokens = response.usage.completion_tokens
    return text, input_tokens, output_tokens


# ---------------------------------------------------------------------------
# Gold loader
# ---------------------------------------------------------------------------
def load_gold_cases(path: Path) -> List[Dict[str, Any]]:
    cases = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            cases.append(json.loads(line))
    return cases


# ---------------------------------------------------------------------------
# Single-model evaluation
# ---------------------------------------------------------------------------
def run_eval_for_model(
    cases: List[Dict[str, Any]],
    model_name: str,
    ollama_url: str,
    ollama_model: str,
    openai_api_key: Optional[str],
) -> List[Dict[str, Any]]:
    """
    Run all gold cases through one model.
    Returns a list of per-query result dicts.
    """
    rows = []
    use_openai = model_name == "gpt-5.4-mini"
    use_groq   = model_name.startswith("groq-") or model_name == "groq"

    for i, ex in enumerate(cases, start=1):
        domain = ex["domain"]
        question = ex["question"]
        gold_sql_raw = ex["gold_sql"]

        pred_sql_raw = ""
        pred_sql_safe = ""
        gold_sql_safe = ""
        status = "UNKNOWN"
        error = ""
        latency_s = 0.0
        input_tokens = 0
        output_tokens = 0
        cost_usd = 0.0

        print(f"  [{i:02d}/{len(cases)}] {model_name} | {domain} | {question[:55]}")

        # 1) Retrieve schema chunks
        try:
            chunks = retrieve_schema_chunks(domain, question, k=TOP_K, persist_dir="data/chroma")
            prompt = build_prompt(domain=domain, question=question, chunks=chunks, dialect="PostgreSQL")
        except Exception as e:
            status = "FAIL_RETRIEVE"
            error = str(e)
            rows.append(_row(model_name, domain, question, status, error,
                             pred_sql_raw, pred_sql_safe, gold_sql_raw, gold_sql_safe,
                             latency_s, input_tokens, output_tokens, cost_usd))
            continue

        # 2) Generate SQL
        t0 = time.perf_counter()
        try:
            if use_openai:
                raw, input_tokens, output_tokens = call_gpt(
                    prompt=prompt, model="gpt-5.4-mini", api_key=openai_api_key
                )
                cost_usd = estimate_cost(input_tokens, output_tokens)
            elif use_groq:
                groq_model = "llama-3.3-70b-versatile" if model_name == "groq" else model_name.replace("groq-", "")
                raw = call_groq(prompt=prompt, model=groq_model)
            else:
                raw = call_ollama(prompt=prompt, model=ollama_model, base_url=ollama_url)

            pred_sql_raw = extract_sql(raw)
            pred_sql_safe = validate_and_fix(pred_sql_raw, limit=LIMIT)
        except Exception as e:
            latency_s = time.perf_counter() - t0
            status = "FAIL_GENERATE_OR_GUARDRAIL"
            error = str(e)
            rows.append(_row(model_name, domain, question, status, error,
                             pred_sql_raw, pred_sql_safe, gold_sql_raw, gold_sql_safe,
                             latency_s, input_tokens, output_tokens, cost_usd))
            continue
        latency_s = time.perf_counter() - t0

        # 3) Guardrails on gold
        try:
            gold_sql_safe = validate_and_fix(gold_sql_raw, limit=LIMIT)
        except Exception as e:
            status = "FAIL_GUARDRAIL_GOLD"
            error = str(e)
            rows.append(_row(model_name, domain, question, status, error,
                             pred_sql_raw, pred_sql_safe, gold_sql_raw, gold_sql_safe,
                             latency_s, input_tokens, output_tokens, cost_usd))
            continue

        # 4) Execute pred
        try:
            p_cols, p_rows = run_sql(dbname=domain, sql=pred_sql_safe, max_rows=LIMIT)
            pred_df = pd.DataFrame(p_rows, columns=p_cols)
        except Exception as e:
            status = "FAIL_EXEC_PRED"
            error = str(e)
            rows.append(_row(model_name, domain, question, status, error,
                             pred_sql_raw, pred_sql_safe, gold_sql_raw, gold_sql_safe,
                             latency_s, input_tokens, output_tokens, cost_usd))
            continue

        # 5) Execute gold
        try:
            g_cols, g_rows = run_sql(dbname=domain, sql=gold_sql_safe, max_rows=LIMIT)
            gold_df = pd.DataFrame(g_rows, columns=g_cols)
        except Exception as e:
            status = "FAIL_EXEC_GOLD"
            error = str(e)
            rows.append(_row(model_name, domain, question, status, error,
                             pred_sql_raw, pred_sql_safe, gold_sql_raw, gold_sql_safe,
                             latency_s, input_tokens, output_tokens, cost_usd))
            continue

        # 6) Compare
        ok = same_result(pred_df, gold_df)
        status = "OK" if ok else "WRONG"
        rows.append(_row(model_name, domain, question, status, "",
                         pred_sql_raw, pred_sql_safe, gold_sql_raw, gold_sql_safe,
                         latency_s, input_tokens, output_tokens, cost_usd,
                         pred_rows=len(pred_df), gold_rows=len(gold_df)))

    return rows


def _row(
    model, domain, question, status, error,
    pred_sql_raw, pred_sql_safe, gold_sql_raw, gold_sql_safe,
    latency_s, input_tokens, output_tokens, cost_usd,
    pred_rows=None, gold_rows=None,
) -> Dict[str, Any]:
    return {
        "model": model,
        "domain": domain,
        "question": question,
        "status": status,
        "error": error,
        "latency_s": round(latency_s, 3),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": round(cost_usd, 6),
        "pred_sql_safe": pred_sql_safe,
        "gold_sql_safe": gold_sql_safe,
        "pred_sql_raw": pred_sql_raw,
        "gold_sql_raw": gold_sql_raw,
        "pred_rows": pred_rows,
        "gold_rows": gold_rows,
    }


# ---------------------------------------------------------------------------
# Summary builder
# ---------------------------------------------------------------------------
def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    records = []
    for model, grp in df.groupby("model"):
        total = len(grp)
        correct = (grp["status"] == "OK").sum()
        exec_fail = grp["status"].isin(["FAIL_EXEC_PRED", "FAIL_GENERATE_OR_GUARDRAIL"]).sum()
        accuracy = correct / total * 100
        avg_latency = grp["latency_s"].mean()
        total_cost = grp["cost_usd"].sum()
        avg_cost = grp["cost_usd"].mean()
        total_tokens = (grp["input_tokens"] + grp["output_tokens"]).sum()

        records.append({
            "model": model,
            "total_queries": total,
            "correct": correct,
            "accuracy_%": round(accuracy, 2),
            "exec_failures": int(exec_fail),
            "avg_latency_s": round(avg_latency, 3),
            "total_cost_usd": round(total_cost, 4),
            "avg_cost_per_query_usd": round(avg_cost, 6),
            "total_tokens": int(total_tokens),
        })
    return pd.DataFrame(records).sort_values("accuracy_%", ascending=False).reset_index(drop=True)


def print_summary(summary_df: pd.DataFrame):
    print("\n" + "=" * 72)
    print("  MODEL COMPARISON SUMMARY")
    print("=" * 72)
    for _, row in summary_df.iterrows():
        cost_str = f"${row['total_cost_usd']:.4f} total  (${row['avg_cost_per_query_usd']:.6f}/query)"
        if row["total_cost_usd"] == 0:
            cost_str = "$0.00  (local inference — no API cost)"
        print(f"\n  Model          : {row['model']}")
        print(f"  Accuracy       : {row['correct']}/{row['total_queries']} ({row['accuracy_%']:.2f}%)")
        print(f"  Exec failures  : {row['exec_failures']}")
        print(f"  Avg latency    : {row['avg_latency_s']:.3f}s per query")
        print(f"  Cost           : {cost_str}")
        if row["total_tokens"] > 0:
            print(f"  Total tokens   : {row['total_tokens']:,}")
    print("\n" + "=" * 72)

    # Head-to-head accuracy gap
    if len(summary_df) == 2:
        top = summary_df.iloc[0]
        bot = summary_df.iloc[1]
        gap = abs(top["accuracy_%"] - bot["accuracy_%"])
        print(f"\n  Accuracy gap   : {gap:.2f}% between {top['model']} and {bot['model']}")
        if bot["model"] == "ollama" or "qwen" in str(bot["model"]).lower():
            print(f"  💡 CV bullet   : \"Matched {top['model']} SQL accuracy within {gap:.0f}% at $0 inference cost using local LLMs.\"")
        else:
            print(f"  💡 CV bullet   : \"Matched {bot['model']} SQL accuracy within {gap:.0f}% at $0 inference cost using local LLMs.\"")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Multi-model NL→SQL benchmark")
    ap.add_argument(
        "--models",
        nargs="+",
        default=["ollama", "gpt-5.4-mini", "groq"],
        help="Models to evaluate: ollama, gpt-5.4-mini, groq, or groq-<model-id>",
    )
    ap.add_argument("--ollama_url", default=os.getenv("OLLAMA_URL", "http://localhost:11434"))
    ap.add_argument("--ollama_model", default=os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b"))
    ap.add_argument("--openai_key", default=os.getenv("OPENAI_API_KEY"))
    args = ap.parse_args()

    if not GOLD_PATH.exists():
        raise FileNotFoundError(f"Missing {GOLD_PATH}")

    cases = load_gold_cases(GOLD_PATH)
    print(f"\nLoaded {len(cases)} gold queries.\n")

    all_rows: List[Dict[str, Any]] = []

    for model_name in args.models:
        print(f"\n{'─'*60}")
        print(f"  Running: {model_name}")
        print(f"{'─'*60}")
        rows = run_eval_for_model(
            cases=cases,
            model_name=model_name,
            ollama_url=args.ollama_url,
            ollama_model=args.ollama_model,
            openai_api_key=args.openai_key,
        )
        all_rows.extend(rows)

    # Save per-query detail
    detail_df = pd.DataFrame(all_rows)
    detail_path = Path("eval/compare_report.csv")
    detail_df.to_csv(detail_path, index=False)
    print(f"\nSaved per-query report: {detail_path.resolve()}")

    # Save + print summary
    summary_df = build_summary(detail_df)
    summary_path = Path("eval/compare_summary.csv")
    summary_df.to_csv(summary_path, index=False)
    print(f"Saved summary:          {summary_path.resolve()}")

    print_summary(summary_df)


if __name__ == "__main__":
    main()
