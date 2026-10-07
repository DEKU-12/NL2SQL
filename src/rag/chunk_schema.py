# src/rag/chunk_schema.py
from __future__ import annotations
import json
import re
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


MAX_LISTED_VALUES = 30   # list every value when a text column has at most this many
MAX_VALUE_LEN = 40
MAX_HINT_LEN = 300       # cap per-column hint so wide tables don't bloat the prompt
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)


def _fmt(v: Any) -> str:
    v = str(v)
    return v if len(v) <= MAX_VALUE_LEN else v[:MAX_VALUE_LEN] + "…"


def column_value_hints(db_path: str | Path, table: str) -> Dict[str, str]:
    """
    Profile a SQLite table so the LLM sees real values, not just names/types:
    - text, <= 30 distinct  -> every value, most frequent first
    - text, repeating       -> 3 most frequent examples (shows formats, e.g. dates)
    - text, ~unique (ids)   -> nothing (examples would be noise)
    - numeric               -> min..max range
    """
    import sqlite3
    hints: Dict[str, str] = {}
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        n_rows = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        for _, col, ctype, *_ in conn.execute(f'PRAGMA table_info("{table}")').fetchall():
            q = f'"{col}"'
            if ctype.upper() in ("INTEGER", "REAL", "NUMERIC"):
                lo, hi = conn.execute(f'SELECT MIN({q}), MAX({q}) FROM "{table}"').fetchone()
                if lo is not None:
                    hints[col] = f"range {_fmt(lo)} to {_fmt(hi)}"
                continue
            n_distinct = conn.execute(f'SELECT COUNT(DISTINCT {q}) FROM "{table}"').fetchone()[0]
            if n_distinct == 0 or n_distinct > n_rows * 0.5:
                continue
            limit = n_distinct if n_distinct <= MAX_LISTED_VALUES else 3
            top = [_fmt(v) for (v,) in conn.execute(
                f'SELECT {q} FROM "{table}" WHERE {q} IS NOT NULL AND {q} != \'\' '
                f'GROUP BY {q} ORDER BY COUNT(*) DESC LIMIT {limit}'
            )]
            if not top or all(_UUID.match(v) for v in top):
                continue
            if n_distinct <= MAX_LISTED_VALUES:
                hints[col] = "values: " + ", ".join(top)
            else:
                hints[col] = f"{n_distinct:,} distinct, e.g. " + ", ".join(top)
            if len(hints[col]) > MAX_HINT_LEN:
                hints[col] = hints[col][:MAX_HINT_LEN].rsplit(", ", 1)[0] + ", …"
    finally:
        conn.close()
    return hints


def table_to_chunk(t: Dict[str, Any], hints: Dict[str, str] | None = None) -> str:
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
        hint = (hints or {}).get(name)
        col_lines.append(f"- {name}{f' ({ctype})' if ctype else ''}{f' — {hint}' if hint else ''}")

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


def chunk_schema(schema_path: str | Path, db_path: str | Path | None = None) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    Returns list of (chunk_id, text, metadata).
    If db_path (SQLite) is given, columns are annotated with real values.
    """
    schema_path = Path(schema_path)
    data = json.loads(schema_path.read_text(encoding="utf-8"))
    tables = normalize_schema(data)

    chunks = []
    for i, t in enumerate(tables):
        table = t.get("table") or f"table_{i}"
        hints = column_value_hints(db_path, table) if db_path else None
        text = table_to_chunk(t, hints)
        chunk_id = f"{table}__schema"
        meta = {"table": table, "kind": "schema"}
        chunks.append((chunk_id, text, meta))
    return chunks
