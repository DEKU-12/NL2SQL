# src/db/sqlite_connect.py
"""
SQLite backend for HF Spaces / environments without PostgreSQL.
Drop-in alternative to src/db/connect.py — activated by USE_SQLITE=true.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, List, Tuple

SQLITE_DIR = Path(__file__).resolve().parents[2] / "data" / "sqlite"

DOMAIN_DB: dict[str, str] = {
    "chinook": "chinook.db",
    "northwind": "northwind.db",
    # dvdrental not available in SQLite bundle — falls back to chinook
}


def get_sqlite_path(domain: str) -> Path:
    db_file = DOMAIN_DB.get(domain, f"{domain}.db")
    path = SQLITE_DIR / db_file
    if not path.exists():
        raise FileNotFoundError(
            f"SQLite DB not found: {path}\n"
            f"Run: python scripts/pg_to_sqlite.py  to generate it."
        )
    return path


def run_sql_sqlite(domain: str, sql: str, max_rows: int = 200) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    """Execute a SELECT query against the bundled SQLite database."""
    # Map dvdrental → chinook in SQLite mode (dvdrental not bundled)
    if domain not in DOMAIN_DB:
        domain = "chinook"

    path = get_sqlite_path(domain)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(sql)
        rows_raw = cur.fetchmany(max_rows)
        if rows_raw:
            cols = list(rows_raw[0].keys())
            rows = [tuple(r) for r in rows_raw]
        else:
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = []
        return cols, rows
    finally:
        conn.close()
