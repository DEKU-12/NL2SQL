# src/rag/chunk_relationships.py
from __future__ import annotations
import json
from typing import List, Dict, Any


def relationship_chunk(schema_json_path: str) -> Dict[str, Any]:
    """
    Creates ONE chunk that lists foreign-key relationships for the whole domain.
    Expects your schema JSON format from scripts/01_extract_schema_pg_anyschema.py:
    { "domain": "...", "tables": [ { "table": "schema.table", "foreign_keys": [...] }, ... ] }
    """
    with open(schema_json_path, "r", encoding="utf-8") as f:
        d = json.load(f)

    domain = d.get("domain", "unknown")
    edges: List[str] = []

    for t in d.get("tables", []):
        src_table = t.get("table")
        for fk in t.get("foreign_keys", []) or []:
            col = fk.get("column")
            ref_table = fk.get("ref_table")
            ref_col = fk.get("ref_column")
            if src_table and col and ref_table and ref_col:
                edges.append(f"{src_table}.{col} -> {ref_table}.{ref_col}")

    edges = sorted(set(edges))
    text = "RELATIONSHIPS (Foreign Keys / Join Map)\n"
    text += f"Domain: {domain}\n\n"
    text += "\n".join(f"- {e}" for e in edges) if edges else "(no foreign keys found)"

    return {
        "id": f"{domain}::relationships",
        "text": text,
        "meta": {"type": "relationships", "domain": domain},
    }
