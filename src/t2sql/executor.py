# src/t2sql/executor.py
from __future__ import annotations
import os
from typing import Any, List, Tuple


def _use_sqlite() -> bool:
    return os.getenv("USE_SQLITE", "").lower() in ("1", "true", "yes")


def get_pg_conn(dbname: str):
    import psycopg2
    host = os.getenv("PGHOST", os.getenv("POSTGRES_HOST", "localhost"))
    port = int(os.getenv("PGPORT", os.getenv("POSTGRES_PORT", "5432")))
    user = os.getenv("PGUSER", os.getenv("POSTGRES_USER", "postgres"))
    pwd  = os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres"))
    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=pwd)


def run_sql(dbname: str, sql: str, max_rows: int = 200) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    """
    Executes SQL and returns (columns, rows).
    Automatically routes to SQLite (USE_SQLITE=true) or PostgreSQL.
    Assumes guardrails already enforced SELECT + LIMIT.
    """
    if _use_sqlite():
        from src.db.sqlite_connect import run_sql_sqlite
        return run_sql_sqlite(dbname, sql, max_rows)

    conn = get_pg_conn(dbname)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchmany(max_rows)
            cols = [d.name for d in cur.description] if cur.description else []
            return cols, rows
    finally:
        conn.close()
