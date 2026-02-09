# eval/run_demo.py
from __future__ import annotations
import json
from pathlib import Path

from src.rag.retrieve import retrieve_schema_chunks
from src.t2sql.prompt_builder import build_prompt
from src.t2sql.generate import call_ollama, extract_sql
from src.t2sql.guardrails import validate_and_fix

DEMO_PATH = Path("eval/demo_questions.json")


def main():
    data = json.loads(DEMO_PATH.read_text(encoding="utf-8"))

    for domain, questions in data.items():
        print("\n" + "="*80)
        print(f"DOMAIN: {domain}")
        print("="*80)

        for q in questions[:10]:
            try:
                chunks = retrieve_schema_chunks(domain, q, k=8, persist_dir="data/chroma")
                prompt = build_prompt(domain, q, chunks, dialect="PostgreSQL")
                raw = call_ollama(prompt, model="llama3.2:3b", base_url="http://localhost:11434")
                sql = validate_and_fix(extract_sql(raw), limit=200)
                print(f"\nQ: {q}\nSQL:\n{sql}\n")
            except Exception as e:
                print(f"\nQ: {q}\nFAILED: {e}\n")


if __name__ == "__main__":
    main()
