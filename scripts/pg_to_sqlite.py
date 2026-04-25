#!/usr/bin/env python3
"""
scripts/pg_to_sqlite.py
Converts PostgreSQL dump files (chinook + northwind) to SQLite databases
bundled in the HF Spaces repo — no Postgres needed.

Usage:
    python scripts/pg_to_sqlite.py
Outputs: data/sqlite/chinook.db  data/sqlite/northwind.db
"""

from __future__ import annotations
import re
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "data" / "sql"
OUT_DIR = ROOT / "data" / "sqlite"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ── Type-name substitution ────────────────────────────────────────────────────
PG_TO_SQLITE_TYPES = [
    (re.compile(r"\bcharacter varying\s*\(\d+\)", re.I), "TEXT"),
    (re.compile(r"\bcharacter varying\b", re.I), "TEXT"),
    (re.compile(r"\bvarchar\s*\(\d+\)", re.I), "TEXT"),
    (re.compile(r"\bvarchar\b", re.I), "TEXT"),
    (re.compile(r"\bnumeric\s*\(\d+,\s*\d+\)", re.I), "REAL"),
    (re.compile(r"\bnumeric\b", re.I), "REAL"),
    (re.compile(r"\bdouble precision\b", re.I), "REAL"),
    (re.compile(r"\bsmallint\b", re.I), "INTEGER"),
    (re.compile(r"\bbigint\b", re.I), "INTEGER"),
    (re.compile(r"\bboolean\b", re.I), "INTEGER"),
    (re.compile(r"\bbytea\b", re.I), "BLOB"),
    (re.compile(r"\btimestamp without time zone\b", re.I), "TEXT"),
    (re.compile(r"\btimestamp with time zone\b", re.I), "TEXT"),
    (re.compile(r"\btimestamp\b", re.I), "TEXT"),
]

# Lines to skip (matched against the whole stripped line)
SKIP_LINE_PATTERNS = [
    re.compile(r"^\s*SET\s+\w", re.I),            # SET statement_timeout = ...
    re.compile(r"^\s*\\[a-z]", re.I),             # psql meta-commands \c, \set
    re.compile(r"^\s*DROP\s+DATABASE\b", re.I),
    re.compile(r"^\s*CREATE\s+DATABASE\b", re.I),
    re.compile(r"^\s*REVOKE\b", re.I),
    re.compile(r"^\s*GRANT\b", re.I),
    re.compile(r"^\s*SELECT\s+pg_catalog", re.I),
    re.compile(r"^\s*CREATE\s+SEQUENCE\b", re.I),
    re.compile(r"^\s*ALTER\s+SEQUENCE\b", re.I),
]


def strip_block_comments(sql: str) -> str:
    """Remove /* ... */ style comments (including multiline)."""
    return re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)


def strip_line_comments(sql: str) -> str:
    """
    Remove -- comments, but only when NOT inside a single-quoted string.
    A naive regex would break strings like 'Quanta Gente Veio ver--Bônus'.
    """
    result: list[str] = []
    i = 0
    in_quote = False
    n = len(sql)
    while i < n:
        ch = sql[i]
        if in_quote:
            result.append(ch)
            if ch == "'":
                # check for escaped quote ''
                if i + 1 < n and sql[i + 1] == "'":
                    result.append("'")
                    i += 2
                    continue
                in_quote = False
        else:
            if ch == "'":
                in_quote = True
                result.append(ch)
            elif ch == "-" and i + 1 < n and sql[i + 1] == "-":
                # skip to end of line
                while i < n and sql[i] != "\n":
                    i += 1
                continue
            else:
                result.append(ch)
        i += 1
    return "".join(result)


def apply_type_subs(sql: str) -> str:
    for pattern, replacement in PG_TO_SQLITE_TYPES:
        sql = pattern.sub(replacement, sql)
    return sql


def fix_boolean_literals(sql: str) -> str:
    """Replace SQL boolean literals true/false with 1/0 in VALUES."""
    # Only replace when not inside identifiers — use word boundaries
    sql = re.sub(r"\btrue\b", "1", sql, flags=re.I)
    sql = re.sub(r"\bfalse\b", "0", sql, flags=re.I)
    return sql


def fix_bytea_literals(sql: str) -> str:
    """Replace PostgreSQL bytea hex literals '\\x...' with NULL."""
    sql = re.sub(r"'\\\\x[0-9a-fA-F]*'", "NULL", sql)
    sql = re.sub(r"'\\x[0-9a-fA-F]*'", "NULL", sql)
    # northwind uses literal '\x' escaped
    sql = sql.replace("'\\x'", "NULL")
    return sql


def fix_nchar_literals(sql: str) -> str:
    """Strip N prefix from N'...' NCHAR literals — SQLite doesn't need it."""
    return re.sub(r"\bN'", "'", sql)


def preprocess(raw: str) -> str:
    """Full preprocessing pipeline."""
    sql = strip_block_comments(raw)
    sql = strip_line_comments(sql)
    sql = apply_type_subs(sql)
    sql = fix_boolean_literals(sql)
    sql = fix_bytea_literals(sql)
    sql = fix_nchar_literals(sql)

    # Strip PG-specific lines
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if any(p.match(stripped) for p in SKIP_LINE_PATTERNS):
            continue
        lines.append(line)

    return "\n".join(lines)


def split_statements(sql_text: str) -> list[str]:
    """
    Robust statement splitter that respects single-quoted strings.
    """
    stmts: list[str] = []
    current: list[str] = []
    in_single_quote = False
    i = 0
    text = sql_text

    while i < len(text):
        ch = text[i]
        if ch == "'" and not in_single_quote:
            in_single_quote = True
            current.append(ch)
        elif ch == "'" and in_single_quote:
            if i + 1 < len(text) and text[i + 1] == "'":
                current.append("''")
                i += 2
                continue
            in_single_quote = False
            current.append(ch)
        elif ch == ";" and not in_single_quote:
            stmt = "".join(current).strip()
            if stmt:
                stmts.append(stmt)
            current = []
        else:
            current.append(ch)
        i += 1

    leftover = "".join(current).strip()
    if leftover:
        stmts.append(leftover)

    return stmts


def filter_statements(stmts: list[str]) -> list[str]:
    """Keep only statements SQLite can handle."""
    good: list[str] = []
    for s in stmts:
        upper = s.upper().strip()
        # Drop ALTER TABLE with constraints (FOREIGN KEY or PRIMARY KEY via ALTER)
        if upper.startswith("ALTER TABLE"):
            if "ADD CONSTRAINT" in upper or "FOREIGN KEY" in upper or "PRIMARY KEY" in upper:
                continue
            # Other ALTER TABLE (e.g. SET DEFAULT) also not supported well — skip
            continue
        # Drop empty / junk (non-SQL text that slipped through)
        if not any(upper.startswith(kw) for kw in (
            "CREATE", "INSERT", "DROP", "BEGIN", "COMMIT", "ROLLBACK", "PRAGMA"
        )):
            continue
        good.append(s)
    return good


def run_statements(conn: sqlite3.Connection, stmts: list[str]) -> int:
    cur = conn.cursor()
    errors = 0
    for stmt in stmts:
        stmt = stmt.strip()
        if not stmt:
            continue
        try:
            cur.execute(stmt)
        except sqlite3.OperationalError as e:
            errors += 1
    conn.commit()
    return errors


def convert(sql_file: Path, db_file: Path) -> None:
    print(f"\n{'='*60}")
    print(f"Converting {sql_file.name}  →  {db_file.name}")

    raw = sql_file.read_text(encoding="utf-8", errors="replace")
    cleaned = preprocess(raw)
    stmts = split_statements(cleaned)
    stmts = filter_statements(stmts)

    print(f"  {len(stmts)} statements to execute")

    if db_file.exists():
        db_file.unlink()

    conn = sqlite3.connect(str(db_file))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    errs = run_statements(conn, stmts)
    conn.close()

    size_kb = db_file.stat().st_size / 1024
    print(f"  Errors: {errs}  |  Size: {size_kb:.0f} KB")

    conn = sqlite3.connect(str(db_file))
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print(f"  Tables ({len(tables)}):")
    total_rows = 0
    for tbl in tables:
        cnt = conn.execute(f'SELECT COUNT(*) FROM "{tbl[0]}"').fetchone()[0]
        total_rows += cnt
        print(f"    {tbl[0]}: {cnt:,} rows")
    print(f"  Total rows: {total_rows:,}")
    conn.close()


if __name__ == "__main__":
    convert(SQL_DIR / "chinook_pg.sql", OUT_DIR / "chinook.db")
    convert(SQL_DIR / "northwind.sql", OUT_DIR / "northwind.db")
    print("\n✅  SQLite databases ready in data/sqlite/")
