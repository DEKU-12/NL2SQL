# src/t2sql/prompt_builder.py
from __future__ import annotations

import re
from pathlib import Path
from typing import List, Dict, Any


SYSTEM_RULES = """You are an expert Text-to-SQL assistant.

Your task:
Generate ONE PostgreSQL SQL query that answers the user's question using the given schema context.

Hard Rules (must follow):
- Output ONLY the SQL query (no explanation, no markdown, no backticks).
- Produce EXACTLY ONE statement (SELECT or WITH only).
- Use ONLY table names and column names that appear in the Schema Context (including RELATIONSHIPS chunk).
- DO NOT invent columns. If a needed column is not present, choose the closest valid column from the Schema Context.
- Use correct JOIN keys based on foreign keys; if foreign keys are not shown, join on matching *_id columns.
- Prefer the join map shown in RELATIONSHIPS chunk when available.
- Qualify columns with table aliases (e.g., c.customer_id).
- Avoid SELECT * unless the question explicitly asks for all fields.
- Always include ORDER BY when asking for top/bottom results.
- Always include LIMIT when returning rows (guardrails will enforce a default).

Additional Accuracy Rules:
- When listing entities, include their primary key ID column (e.g., customer_id, film_id, album_id, artist_id, category_id, product_id, supplier_id, employee_id/staff_id).
- If the question says "top N", set LIMIT N exactly.
- If the question asks for a single-value metric (average/total/count/sum/min/max), return ONE row with an aggregate (avoid per-group output unless explicitly requested).

If the question cannot be answered using the Schema Context, output exactly:
SELECT 'INSUFFICIENT_SCHEMA' AS error;
"""


def domain_glossary(domain: str) -> str:
    d = domain.lower()
    if d == "chinook":
        return """Domain Glossary (Chinook):
- spend / customer spend / revenue: invoice.total
- sales: invoice_line.unit_price * invoice_line.quantity
- common joins:
  invoice.customer_id = customer.customer_id
  invoice.invoice_id = invoice_line.invoice_id
  invoice_line.track_id = track.track_id
  track.album_id = album.album_id
  album.artist_id = artist.artist_id

"""
    if d == "dvdrental":
        return """Domain Glossary (DVDRental):
- revenue: payment.amount
- rentals: COUNT(rental.rental_id)
- common joins:
  rental.inventory_id = inventory.inventory_id
  inventory.film_id = film.film_id
  payment.customer_id = customer.customer_id
  customer.address_id -> address.city_id -> city.country_id -> country.country_id

"""
    if d == "northwind":
        return """Domain Glossary (Northwind):
- sales / revenue / order value: order_details.unit_price * order_details.quantity * (1 - order_details.discount)
- common joins:
  orders.order_id = order_details.order_id
  orders.customer_id = customers.customer_id
  order_details.product_id = products.product_id
  products.category_id = categories.category_id
  products.supplier_id = suppliers.supplier_id

"""
    return ""


def detect_top_n(question: str) -> int | None:
    m = re.search(r"\btop\s+(\d+)\b", question.lower())
    return int(m.group(1)) if m else None


def looks_scalar(question: str) -> bool:
    q = question.lower()
    scalar_markers = [
        "average", "avg", "total", "sum", "count", "number of",
        "maximum", "minimum", "max", "min", "revenue"
    ]
    if "top " in q:
        return False
    return any(m in q for m in scalar_markers)


def extra_guidance(domain: str, question: str) -> str:
    tips = []

    tips.append(
        "When listing entities, always include their ID column:\n"
        "- customers -> customer_id\n"
        "- films -> film_id\n"
        "- albums -> album_id\n"
        "- artists -> artist_id\n"
        "- categories -> category_id\n"
        "- employees/staff -> employee_id/staff_id\n"
        "- products -> product_id\n"
        "- suppliers -> supplier_id"
    )

    n = detect_top_n(question)
    if n is not None:
        tips.append(f'This is a "top {n}" question: set LIMIT {n} exactly.')

    if looks_scalar(question):
        tips.append("This is a single-value question: return ONE row with an aggregate (no per-group output).")

    return "Extra Guidance:\n" + "\n".join(f"- {t}" for t in tips) + "\n\n"


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
    return f"Few-shot Examples (follow this style):\n{txt}\n\n"


def build_prompt(domain: str, question: str, chunks: List[Dict[str, Any]], dialect: str = "PostgreSQL") -> str:
    context = "\n\n---\n\n".join([c["text"] for c in chunks]) if chunks else "(no schema context found)"
    few_shot = _load_few_shot(domain)
    gloss = domain_glossary(domain)
    guidance = extra_guidance(domain, question)

    prompt = f"""{SYSTEM_RULES}

SQL Dialect: {dialect}
Domain: {domain}

{few_shot}{gloss}{guidance}Schema Context:
{context}

User Question:
{question}

SQL:
"""
    return prompt
