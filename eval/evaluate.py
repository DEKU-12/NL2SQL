# eval/evaluate.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from src.rag.retrieve import retrieve_schema_chunks
from src.t2sql.prompt_builder import build_prompt
from src.t2sql.generate import call_ollama, extract_sql
from src.t2sql.guardrails import validate_and_fix
from src.t2sql.executor import run_sql


GOLD_PATH = Path("eval/gold.jsonl")

# Config (edit here if you want)
OLLAMA_URL = "http://localhost:11434"
OLLAMA_MODEL = "llama3.2:3b"
TOP_K = 12          # bumped from 8 → 12 for better retrieval
LIMIT = 200
ROUND_DECIMALS = 2  # float rounding for result comparison


def load_gold_cases(path: Path) -> List[Dict[str, Any]]:
    cases: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            cases.append(json.loads(line))
    return cases


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize results for stable comparison:
    - lowercase column names
    - sort columns
    - round numeric columns
    - convert all to string
    - sort rows lexicographically
    """
    df2 = df.copy()

    # lowercase + sort columns
    df2.columns = [str(c).lower() for c in df2.columns]
    df2 = df2.sort_index(axis=1)

    # round numeric columns (helps float/decimal noise)
    for c in df2.columns:
        try:
            if pd.api.types.is_numeric_dtype(df2[c]):
                df2[c] = df2[c].round(ROUND_DECIMALS)
        except Exception:
            pass

    # cast to string for stable equality
    df2 = df2.astype(str)

    # sort rows
    if len(df2.columns) > 0 and len(df2) > 0:
        df2 = df2.sort_values(list(df2.columns)).reset_index(drop=True)
    else:
        df2 = df2.reset_index(drop=True)

    return df2


def same_result(df_pred: pd.DataFrame, df_gold: pd.DataFrame) -> bool:
    """
    Result equivalence check.
    NOTE: Requires same number of columns. (Keeps it simple + reliable.)
    """
    try:
        a = normalize_df(df_pred)
        b = normalize_df(df_gold)

        # must have same shape
        if a.shape != b.shape:
            return False

        return a.equals(b)
    except Exception:
        return False


def main():
    if not GOLD_PATH.exists():
        raise FileNotFoundError(f"Missing {GOLD_PATH}. Create it first (eval/gold.jsonl).")

    cases = load_gold_cases(GOLD_PATH)

    total = len(cases)
    guardrail_pass_pred = 0
    guardrail_pass_gold = 0

    executed_pred = 0
    executed_gold = 0
    executed_both = 0

    exec_correct_all = 0         # correct out of total
    exec_correct_on_executed = 0 # correct out of executed_both

    report_rows: List[Dict[str, Any]] = []

    for ex in cases:
        domain = ex["domain"]
        question = ex["question"]
        gold_sql_raw = ex["gold_sql"]

        pred_sql_raw = ""
        pred_sql_safe = ""
        gold_sql_safe = ""
        status = "UNKNOWN"
        error = ""

        # 1) Generate predicted SQL
        try:
            chunks = retrieve_schema_chunks(domain, question, k=TOP_K, persist_dir="data/chroma")
            prompt = build_prompt(domain=domain, question=question, chunks=chunks, dialect="PostgreSQL")

            raw = call_ollama(prompt=prompt, model=OLLAMA_MODEL, base_url=OLLAMA_URL)
            pred_sql_raw = extract_sql(raw)

            pred_sql_safe = validate_and_fix(pred_sql_raw, limit=LIMIT)
            guardrail_pass_pred += 1

        except Exception as e:
            status = "FAIL_GENERATE_OR_GUARDRAIL_PRED"
            error = str(e)
            report_rows.append({
                "domain": domain,
                "question": question,
                "status": status,
                "error": error,
                "pred_sql_raw": pred_sql_raw,
                "pred_sql_safe": pred_sql_safe,
                "gold_sql_raw": gold_sql_raw,
                "gold_sql_safe": "",
            })
            continue

        # 2) Apply guardrails to gold too (IMPORTANT for fair comparison)
        try:
            gold_sql_safe = validate_and_fix(gold_sql_raw, limit=LIMIT)
            guardrail_pass_gold += 1
        except Exception as e:
            status = "FAIL_GUARDRAIL_GOLD"
            error = str(e)
            report_rows.append({
                "domain": domain,
                "question": question,
                "status": status,
                "error": error,
                "pred_sql_raw": pred_sql_raw,
                "pred_sql_safe": pred_sql_safe,
                "gold_sql_raw": gold_sql_raw,
                "gold_sql_safe": gold_sql_safe,
            })
            continue

        # 3) Execute predicted
        try:
            p_cols, p_rows = run_sql(dbname=domain, sql=pred_sql_safe, max_rows=LIMIT)
            executed_pred += 1
            pred_df = pd.DataFrame(p_rows, columns=p_cols)
        except Exception as e:
            status = "FAIL_EXEC_PRED"
            error = str(e)
            report_rows.append({
                "domain": domain,
                "question": question,
                "status": status,
                "error": error,
                "pred_sql_raw": pred_sql_raw,
                "pred_sql_safe": pred_sql_safe,
                "gold_sql_raw": gold_sql_raw,
                "gold_sql_safe": gold_sql_safe,
            })
            continue

        # 4) Execute gold
        try:
            g_cols, g_rows = run_sql(dbname=domain, sql=gold_sql_safe, max_rows=LIMIT)
            executed_gold += 1
            gold_df = pd.DataFrame(g_rows, columns=g_cols)
        except Exception as e:
            status = "FAIL_EXEC_GOLD"
            error = str(e)
            report_rows.append({
                "domain": domain,
                "question": question,
                "status": status,
                "error": error,
                "pred_sql_raw": pred_sql_raw,
                "pred_sql_safe": pred_sql_safe,
                "gold_sql_raw": gold_sql_raw,
                "gold_sql_safe": gold_sql_safe,
            })
            continue

        executed_both += 1

        # 5) Compare results
        ok = same_result(pred_df, gold_df)
        if ok:
            exec_correct_all += 1
            exec_correct_on_executed += 1
            status = "OK"
        else:
            status = "WRONG"

        report_rows.append({
            "domain": domain,
            "question": question,
            "status": status,
            "error": "",
            "pred_sql_raw": pred_sql_raw,
            "pred_sql_safe": pred_sql_safe,
            "gold_sql_raw": gold_sql_raw,
            "gold_sql_safe": gold_sql_safe,
            "pred_cols": list(pred_df.columns),
            "gold_cols": list(gold_df.columns),
            "pred_rows": len(pred_df),
            "gold_rows": len(gold_df),
        })

    # Summary
    print("\n=== EVALUATION SUMMARY ===")
    print(f"Total cases:                 {total}")
    print(f"Guardrail pass (pred):       {guardrail_pass_pred} ({guardrail_pass_pred/total:.2%})")
    print(f"Guardrail pass (gold):       {guardrail_pass_gold} ({guardrail_pass_gold/total:.2%})")
    print(f"Pred SQL executed:           {executed_pred} ({executed_pred/total:.2%})")
    print(f"Gold SQL executed:           {executed_gold} ({executed_gold/total:.2%})")
    print(f"Executed BOTH pred+gold:     {executed_both} ({executed_both/total:.2%})")
    print(f"Execution accuracy (overall): {exec_correct_all} ({exec_correct_all/total:.2%})")
    if executed_both > 0:
        print(f"Accuracy on executed-both:    {exec_correct_on_executed} ({exec_correct_on_executed/executed_both:.2%})")

    out_df = pd.DataFrame(report_rows)
    out_path = Path("eval/report.csv")
    out_df.to_csv(out_path, index=False)
    print(f"Saved detailed report: {out_path.resolve()}")


if __name__ == "__main__":
    main()
