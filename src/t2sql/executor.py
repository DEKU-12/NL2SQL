# src/t2sql/executor.py
from __future__ import annotations
import os
from typing import Any, Dict, List, Tuple
import psycopg2


def get_pg_conn(dbname: str):
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    user = os.getenv("PGUSER", "postgres")
    pwd  = os.getenv("PGPASSWORD", "postgres")
    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=pwd)


def run_sql(dbname: str, sql: str, max_rows: int = 200) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    """
    Executes SQL and returns (columns, rows). Assumes guardrails already enforced SELECT + LIMIT.
    """
    conn = get_pg_conn(dbname)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchmany(max_rows)
            cols = [d.name for d in cur.description] if cur.description else []
            return cols, rows
    finally:
        conn.close()
