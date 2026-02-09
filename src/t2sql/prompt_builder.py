# src/t2sql/prompt_builder.py
from __future__ import annotations
from typing import List, Dict, Any


SYSTEM_RULES = """You are an expert Text-to-SQL assistant.
Generate a single SQL query that answers the user's question.

Rules:
- Output ONLY the SQL query. No explanations. No markdown.
- Use ONLY tables/columns that appear in the provided schema context.
- Use correct join keys based on foreign keys / column names.
- Prefer explicit column names (avoid SELECT *) unless necessary.
- Always include LIMIT when returning rows.
- If the question cannot be answered using the schema context, output:
  SELECT 'INSUFFICIENT_SCHEMA' AS error;
"""


def build_prompt(domain: str, question: str, chunks: List[Dict[str, Any]], dialect: str = "PostgreSQL") -> str:
    context = "\n\n---\n\n".join([c["text"] for c in chunks]) if chunks else "(no schema context found)"

    prompt = f"""{SYSTEM_RULES}

SQL Dialect: {dialect}
Domain: {domain}

Schema Context:
{context}

User Question:
{question}

SQL:
"""
    return prompt
