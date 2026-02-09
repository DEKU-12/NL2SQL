from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Tuple

import sqlglot
from sqlalchemy import text

from src.config.load_config import get_postgres_settings
from src.db.connect import get_engine

DISALLOWED_KEYWORDS = {
    "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "GRANT", "REVOKE", "CREATE"
}


@dataclass
class QueryResult:
    columns: List[str]
    rows: List[Tuple[Any, ...]]
    rowcount: int


def _basic_blocklist(sql: str) -> None:
    upper = sql.upper()
    for kw in DISALLOWED_KEYWORDS:
        if kw in upper:
            raise ValueError(f"Blocked keyword detected: {kw}")


def _select_only(sql: str) -> None:
    try:
        parsed = sqlglot.parse_one(sql, read="postgres")
    except Exception as e:
        raise ValueError(f"SQL parse error: {e}")

    if parsed.__class__.__name__ not in {"Select", "With"}:
        raise ValueError(f"Only SELECT/WITH allowed. Got: {parsed.__class__.__name__}")


def _ensure_limit(sql: str, default_limit: int) -> str:
    upper = sql.upper()
    if " LIMIT " in upper:
        return sql.strip().rstrip(";") + ";"
    return sql.strip().rstrip(";") + f"\nLIMIT {default_limit};"


def run_query(domain: str, sql: str) -> QueryResult:
    settings = get_postgres_settings()

    _basic_blocklist(sql)
    _select_only(sql)
    sql = _ensure_limit(sql, settings.max_rows)

    engine = get_engine(domain)
    with engine.connect() as conn:
        res = conn.execute(text(sql))
        rows = res.fetchall()
        cols = list(res.keys())

    return QueryResult(columns=cols, rows=rows, rowcount=len(rows))
