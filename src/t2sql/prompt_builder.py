# src/t2sql/prompt_builder.py
from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any


SYSTEM_RULES = """You are an expert Text-to-SQL assistant.

Your task:
Generate ONE PostgreSQL SQL query that answers the user's question using the given schema context.

Hard Rules (must follow):
- Output ONLY the SQL query (no explanation, no markdown, no backticks).
- Produce EXACTLY ONE statement (SELECT or WITH only).
- Use ONLY table names and column names that appear in the Schema Context.
- DO NOT invent columns. If a needed column is not present, choose the closest valid column from the Schema Context.
- Use correct JOIN keys based on foreign keys; if foreign keys are not shown, join on matching *_id columns.
- Qualify columns with table aliases (e.g., c.customer_id).
- Avoid SELECT * unless the question explicitly asks for all fields.
- Always include ORDER BY when asking for top/bottom results.
- Always include LIMIT when returning rows (your guardrails will enforce it).

If the question cannot be answered using the Schema Context, output exactly:
SELECT 'INSUFFICIENT_SCHEMA' AS error;
"""


def _load_few_shot(domain: str) -> str:
    """
    Loads few-shot examples from:
    data/examples/<domain>.txt
    Format:
      Q: ...
      SQL: ...
    """
    p = Path("data/examples") / f"{domain}.txt"
    if not p.exists():
        return ""
    txt = p.read_text(encoding="utf-8").strip()
    if not txt:
        return ""
    return f"Few-shot Examples (follow this style):\n{txt}\n"


def build_prompt(domain: str, question: str, chunks: List[Dict[str, Any]], dialect: str = "PostgreSQL") -> str:
    context = "\n\n---\n\n".join([c["text"] for c in chunks]) if chunks else "(no schema context found)"
    few_shot = _load_few_shot(domain)

    prompt = f"""{SYSTEM_RULES}

SQL Dialect: {dialect}
Domain: {domain}

{few_shot}Schema Context:
{context}

User Question:
{question}

SQL:
"""
    return prompt
