# src/t2sql/generate.py
from __future__ import annotations
import argparse
import os
import requests

from src.rag.retrieve import retrieve_schema_chunks
from src.t2sql.prompt_builder import build_prompt
from src.t2sql.guardrails import validate_and_fix, strip_code_fences


def call_ollama(prompt: str, model: str, base_url: str = "http://localhost:11434") -> str:
    url = f"{base_url}/api/chat"
    payload = {
        "model": model,
        "stream": False,
        "messages": [
            {"role": "system", "content": "You are a Text-to-SQL generator. Output ONLY SQL."},
            {"role": "user", "content": prompt},
        ],
        "options": {"temperature": 0.1, "num_predict": 512},
    }
    r = requests.post(url, json=payload, timeout=120)
    r.raise_for_status()
    return r.json()["message"]["content"]



def extract_sql(text: str) -> str:
    t = strip_code_fences(text).strip()

    # common patterns: "SQLQuery:" or "SQL:"
    for prefix in ["SQLQuery:", "SQL:", "Query:"]:
        if t.lower().startswith(prefix.lower()):
            t = t[len(prefix):].strip()

    # keep only first statement-ish block
    return t.strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--domain", required=True)
    ap.add_argument("--question", required=True)
    ap.add_argument("--k", type=int, default=6)
    ap.add_argument("--dialect", type=str, default="PostgreSQL")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--persist_dir", type=str, default="data/chroma")
    ap.add_argument("--model", type=str, default=os.getenv("OLLAMA_MODEL", "llama3.1:8b-instruct"))
    ap.add_argument("--base_url", type=str, default=os.getenv("OLLAMA_URL", "http://localhost:11434"))
    ap.add_argument("--show_context", action="store_true")
    args = ap.parse_args()

    chunks = retrieve_schema_chunks(
        domain=args.domain,
        question=args.question,
        k=args.k,
        persist_dir=args.persist_dir,
    )

    prompt = build_prompt(domain=args.domain, question=args.question, chunks=chunks, dialect=args.dialect)

    if args.show_context:
        print("\n===== PROMPT =====\n")
        print(prompt)
        print("\n===== END PROMPT =====\n")

    raw = call_ollama(prompt=prompt, model=args.model, base_url=args.base_url)
    sql = extract_sql(raw)

    safe_sql = validate_and_fix(sql, limit=args.limit)
    print(safe_sql)


if __name__ == "__main__":
    main()
