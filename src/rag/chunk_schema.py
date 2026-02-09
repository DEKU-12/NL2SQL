# src/rag/chunk_schema.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _as_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def normalize_schema(schema: Any) -> List[Dict[str, Any]]:
    """
    Accepts multiple schema JSON formats and normalizes to:
    [
      {
        "table": "table_name",
        "columns": [{"name":"col", "type":"TEXT", "pk":bool}],
        "primary_key": ["col1", ...],
        "foreign_keys": [{"column":"x","ref_table":"t","ref_column":"id"}],
        "description": "..."
      }, ...
    ]
    """
    # Format 1 (preferred): {"tables":[{...}, ...]}
    if isinstance(schema, dict) and "tables" in schema and isinstance(schema["tables"], list):
        tables_out = []
        for t in schema["tables"]:
            table = t.get("table") or t.get("name") or t.get("table_name")
            cols = t.get("columns", [])
            pk = t.get("primary_key") or t.get("pk") or []
            fks = t.get("foreign_keys") or t.get("fks") or []
            tables_out.append({
                "table": table,
                "columns": cols,
                "primary_key": pk,
                "foreign_keys": fks,
                "description": t.get("description", "")
            })
        return tables_out

    # Format 2: {"tables": {"tableA": {"columns":[...], ...}, "tableB": ...}}
    if isinstance(schema, dict) and "tables" in schema and isinstance(schema["tables"], dict):
        out = []
        for table, info in schema["tables"].items():
            cols = info.get("columns", [])
            pk = info.get("primary_key") or info.get("pk") or []
            fks = info.get("foreign_keys") or info.get("fks") or []
            out.append({
                "table": table,
                "columns": cols,
                "primary_key": pk,
                "foreign_keys": fks,
                "description": info.get("description", "")
            })
        return out

    # Format 3: {"tableA":["col1","col2"], "tableB":[...]} or {"tableA":{...}}
    if isinstance(schema, dict):
        out = []
        for table, info in schema.items():
            if table in ("db", "database", "domain", "meta"):
                continue
            cols = []
            if isinstance(info, list):
                cols = [{"name": c, "type": ""} for c in info]
            elif isinstance(info, dict) and "columns" in info:
                cols = info.get("columns", [])
            elif isinstance(info, dict):
                # maybe {"col":"type"}
                if all(isinstance(v, str) for v in info.values()):
                    cols = [{"name": k, "type": v} for k, v in info.items()]
            out.append({
                "table": table,
                "columns": cols,
                "primary_key": [],
                "foreign_keys": [],
                "description": ""
            })
        return out

    raise ValueError("Unsupported schema JSON format. Expected a dict with tables.")


def table_to_chunk(t: Dict[str, Any]) -> str:
    table = t["table"]
    desc = (t.get("description") or "").strip()
    cols = t.get("columns", [])
    pk = _as_list(t.get("primary_key"))
    fks = _as_list(t.get("foreign_keys"))

    col_lines = []
    for c in cols:
        name = c.get("name") if isinstance(c, dict) else str(c)
        ctype = ""
        if isinstance(c, dict):
            ctype = c.get("type", "") or c.get("dtype", "") or ""
        col_lines.append(f"- {name}{f' ({ctype})' if ctype else ''}")

    fk_lines = []
    for fk in fks:
        if not isinstance(fk, dict):
            continue
        col = fk.get("column") or fk.get("from")
        rt = fk.get("ref_table") or fk.get("to_table") or fk.get("table")
        rc = fk.get("ref_column") or fk.get("to_column") or fk.get("column_ref")
        if col and rt and rc:
            fk_lines.append(f"- {col} -> {rt}.{rc}")

    chunk = []
    chunk.append(f"TABLE: {table}")
    if desc:
        chunk.append(f"DESCRIPTION: {desc}")
    chunk.append("COLUMNS:")
    chunk.extend(col_lines if col_lines else ["- (none found)"])

    if pk:
        chunk.append(f"PRIMARY KEY: {', '.join(pk)}")
    if fk_lines:
        chunk.append("FOREIGN KEYS:")
        chunk.extend(fk_lines)

    return "\n".join(chunk)


def chunk_schema(schema_path: str | Path) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    Returns list of (chunk_id, text, metadata)
    """
    schema_path = Path(schema_path)
    data = json.loads(schema_path.read_text(encoding="utf-8"))
    tables = normalize_schema(data)

    chunks = []
    for i, t in enumerate(tables):
        table = t.get("table") or f"table_{i}"
        text = table_to_chunk(t)
        chunk_id = f"{table}__schema"
        meta = {"table": table, "kind": "schema"}
        chunks.append((chunk_id, text, meta))
    return chunks
