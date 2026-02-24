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

# Config
OLLAMA_URL = "http://localhost:11434"
OLLAMA_MODEL = "llama3.2:3b"
TOP_K = 12
LIMIT = 200
ROUND_DECIMALS = 2


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

    df2.columns = [str(c).lower() for c in df2.columns]
    df2 = df2.sort_index(axis=1)

    # round numeric cols
    for c in df2.columns:
        try:
            if pd.api.types.is_numeric_dtype(df2[c]):
                df2[c] = df2[c].round(ROUND_DECIMALS)
        except Exception:
            pass

    df2 = df2.astype(str)

    if len(df2.columns) > 0 and len(df2) > 0:
        df2 = df2.sort_values(list(df2.columns)).reset_index(drop=True)
    else:
        df2 = df2.reset_index(drop=True)

    return df2


def same_result(df_pred: pd.DataFrame, df_gold: pd.DataFrame) -> bool:
    """
    Fair equivalence:
    - If both scalar (1x1): compare value (rounded)
    - Else: compare common columns only
    - Compare only top N rows where N=min(rows_pred, rows_gold) (handles LIMIT differences)
    """
    try:
        # scalar compare
        if df_pred.shape == (1, 1) and df_gold.shape == (1, 1):
            a = df_pred.iloc[0, 0]
            b = df_gold.iloc[0, 0]
            try:
                return round(float(a), ROUND_DECIMALS) == round(float(b), ROUND_DECIMALS)
            except Exception:
                return str(a).strip() == str(b).strip()

        a = normalize_df(df_pred)
        b = normalize_df(df_gold)

        # align on common columns
        common_cols = [c for c in a.columns if c in b.columns]
        if not common_cols:
            return False

        a2 = a[common_cols].reset_index(drop=True)
        b2 = b[common_cols].reset_index(drop=True)

        # compare only up to min row count
        n = min(len(a2), len(b2))
        a2 = a2.head(n).reset_index(drop=True)
        b2 = b2.head(n).reset_index(drop=True)

        return a2.equals(b2)
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

    exec_correct_all = 0
    exec_correct_on_executed = 0

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

        # 2) Guardrails on gold too
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

        # 3) Execute pred
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
