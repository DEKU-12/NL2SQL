# eval/retrieval_recall.py
"""
Retrieval-only eval (no LLM calls, free): for each gold query, are all tables
used by the gold SQL among the top-K retrieved schema chunks?

    python eval/retrieval_recall.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import sqlglot
from sqlglot import exp

from src.rag.retrieve import retrieve_schema_chunks

GOLD_PATH = Path("eval/gold.jsonl")
MAX_K = 10  # largest domain (olist): 9 table chunks + 1 relationships chunk


def gold_tables(sql: str) -> set[str]:
    tree = sqlglot.parse_one(sql, read="sqlite")
    ctes = {c.alias_or_name for c in tree.find_all(exp.CTE)}
    return {t.name for t in tree.find_all(exp.Table)} - ctes


def main():
    cases = [json.loads(l) for l in GOLD_PATH.read_text().splitlines() if l.strip()]
    hits = {k: 0 for k in range(1, MAX_K + 1)}
    sent = {k: 0 for k in hits}   # tables actually put in the prompt (incl. join-path bridges)
    for ex in cases:
        need = gold_tables(ex["gold_sql"])
        for k in hits:
            got = {c["meta"].get("table") for c in
                   retrieve_schema_chunks(ex["domain"], ex["question"], k=k)
                   if c["meta"].get("type") != "relationships"}
            hits[k] += need <= got
            sent[k] += len(got)

    print(f"Table recall over {len(cases)} gold queries (all needed tables retrieved):")
    for k, n in hits.items():
        print(f"  K={k}: {n}/{len(cases)} ({n/len(cases):.1%})  avg tables in prompt: {sent[k]/len(cases):.1f}")


if __name__ == "__main__":
    main()
