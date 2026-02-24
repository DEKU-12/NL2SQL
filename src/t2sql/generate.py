# src/t2sql/generate.py
from __future__ import annotations

import argparse
import os
import requests

from src.rag.retrieve import retrieve_schema_chunks
from src.t2sql.prompt_builder import build_prompt
from src.t2sql.guardrails import validate_and_fix, strip_code_fences
from src.t2sql.executor import run_sql


def call_ollama(prompt: str, model: str, base_url: str = "http://localhost:11434") -> str:
    """
    Uses Ollama /api/chat (modern Ollama versions).
    """
    url = f"{base_url}/api/chat"
    payload = {
        "model": model,
        "stream": False,
        "messages": [
            {"role": "system", "content": "You are a precise Text-to-SQL generator. Output ONLY SQL."},
            {"role": "user", "content": prompt},
        ],
        "options": {"temperature": 0.1, "num_predict": 512},
    }
    r = requests.post(url, json=payload, timeout=180)
    r.raise_for_status()
    data = r.json()
    return data["message"]["content"]


def extract_sql(text: str) -> str:
    t = strip_code_fences(text).strip()
    for prefix in ["SQLQuery:", "SQL:", "Query:"]:
        if t.lower().startswith(prefix.lower()):
            t = t[len(prefix):].strip()
    return t.strip()


def build_fix_prompt(domain: str, dialect: str, schema_context: str, question: str, bad_sql: str, error_msg: str) -> str:
    return f"""You are an expert SQL debugger.

Task:
Fix the SQL query so it executes successfully and answers the question.

Rules:
- Output ONLY the corrected SQL (no markdown, no explanation).
- Produce exactly ONE statement (SELECT or WITH only).
- Use ONLY tables/columns that appear in the Schema Context.
- Do NOT invent columns. If a column does not exist, replace it with a valid one from schema.
- Keep the intent of the question.
- Ensure the final query includes LIMIT.

SQL Dialect: {dialect}
Domain: {domain}

Schema Context:
{schema_context}

User Question:
{question}

Bad SQL:
{bad_sql}

Database Error:
{error_msg}

Corrected SQL:
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--domain", required=True)
    ap.add_argument("--question", required=True)
    ap.add_argument("--k", type=int, default=12)
    ap.add_argument("--dialect", type=str, default="PostgreSQL")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--persist_dir", type=str, default="data/chroma")
    ap.add_argument("--model", type=str, default=os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b"))
    ap.add_argument("--base_url", type=str, default=os.getenv("OLLAMA_URL", "http://localhost:11434"))
    ap.add_argument("--retries", type=int, default=2, help="how many self-correction retries on SQL execution error")
    ap.add_argument("--show_context", action="store_true")
    args = ap.parse_args()

    # 1) Retrieve schema chunks
    chunks = retrieve_schema_chunks(
        domain=args.domain,
        question=args.question,
        k=args.k,
        persist_dir=args.persist_dir,
    )
    schema_context = "\n\n---\n\n".join([c["text"] for c in chunks]) if chunks else "(no schema context found)"

    # 2) Build initial prompt + generate
    prompt = build_prompt(domain=args.domain, question=args.question, chunks=chunks, dialect=args.dialect)

    if args.show_context:
        print("\n===== PROMPT =====\n")
        print(prompt)
        print("\n===== END PROMPT =====\n")

    raw = call_ollama(prompt=prompt, model=args.model, base_url=args.base_url)
    sql = validate_and_fix(extract_sql(raw), limit=args.limit)

    # 3) Self-correction loop: execute -> if fails -> ask model to fix
    last_err = None
    for attempt in range(args.retries + 1):
        try:
            # If this executes, we accept it (we don't need results here; just validation)
            run_sql(dbname=args.domain, sql=sql, max_rows=args.limit)
            print(sql)
            return
        except Exception as e:
            last_err = str(e)
            if attempt >= args.retries:
                break

            fix_prompt = build_fix_prompt(
                domain=args.domain,
                dialect=args.dialect,
                schema_context=schema_context,
                question=args.question,
                bad_sql=sql,
                error_msg=last_err,
            )
            fixed_raw = call_ollama(prompt=fix_prompt, model=args.model, base_url=args.base_url)
            sql = validate_and_fix(extract_sql(fixed_raw), limit=args.limit)

    # If we get here, all retries failed
    raise RuntimeError(f"SQL failed after {args.retries+1} attempts. Last error:\n{last_err}\nLast SQL:\n{sql}")


if __name__ == "__main__":
    main()
