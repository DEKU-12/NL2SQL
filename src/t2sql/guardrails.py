# src/t2sql/guardrails.py
from __future__ import annotations
import re
from typing import Tuple


FORBIDDEN = [
    "insert", "update", "delete", "drop", "alter", "truncate", "create",
    "grant", "revoke", "commit", "rollback", "vacuum", "pragma", "attach", "detach",
    "copy", "call", "execute", "merge"
]


def strip_code_fences(sql: str) -> str:
    s = sql.strip()
    # remove ```sql ... ```
    s = re.sub(r"^```(?:sql)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```$", "", s)
    return s.strip()


def is_select_only(sql: str) -> bool:
    s = strip_code_fences(sql).strip()

    # block multiple statements
    # allow a single trailing ';'
    if ";" in s[:-1]:
        return False

    low = s.lower()

    # must start with SELECT or WITH
    if not (low.startswith("select") or low.startswith("with")):
        return False

    # block forbidden keywords anywhere
    for kw in FORBIDDEN:
        if re.search(rf"\b{re.escape(kw)}\b", low):
            return False

    return True


def enforce_limit(sql: str, limit: int = 200) -> str:
    s = strip_code_fences(sql).strip()
    # if LIMIT exists, clamp it
    m = re.search(r"\blimit\s+(\d+)\b", s, flags=re.IGNORECASE)
    if m:
        n = int(m.group(1))
        if n > limit:
            s = re.sub(r"\blimit\s+\d+\b", f"LIMIT {limit}", s, flags=re.IGNORECASE)
        return s

    # no limit: append safely (remove trailing ;)
    s = s.rstrip().rstrip(";")
    return f"{s}\nLIMIT {limit};"


def validate_and_fix(sql: str, limit: int = 200) -> str:
    s = strip_code_fences(sql)
    if not is_select_only(s):
        raise ValueError("Guardrails: rejected (must be SELECT/WITH only; no DDL/DML; single statement).")
    return enforce_limit(s, limit=limit)
