from __future__ import annotations

from typing import Any, Dict

from sqlalchemy import text

from src.db.connect import get_engine


def extract_schema(domain: str) -> Dict[str, Any]:
    engine = get_engine(domain)
    schema: Dict[str, Any] = {"domain": domain, "tables": []}

    with engine.connect() as conn:
        tables = conn.execute(
            text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """)
        ).fetchall()

        for (table_name,) in tables:
            cols = conn.execute(
                text("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema='public'
                      AND table_name=:t
                    ORDER BY ordinal_position;
                """),
                {"t": table_name},
            ).fetchall()

            columns = [{"name": c, "type": dt} for (c, dt) in cols]

            pk_rows = conn.execute(
                text("""
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                      AND tc.table_schema = 'public'
                      AND tc.table_name = :t
                    ORDER BY kcu.ordinal_position;
                """),
                {"t": table_name},
            ).fetchall()
            primary_key = [r[0] for r in pk_rows]

            fk_rows = conn.execute(
                text("""
                    SELECT
                      kcu.column_name AS fk_column,
                      ccu.table_name  AS ref_table,
                      ccu.column_name AS ref_column
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage ccu
                      ON ccu.constraint_name = tc.constraint_name
                     AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                      AND tc.table_schema = 'public'
                      AND tc.table_name = :t
                    ORDER BY kcu.column_name;
                """),
                {"t": table_name},
            ).fetchall()

            foreign_keys = [
                {"column": fk_col, "ref_table": ref_table, "ref_column": ref_col}
                for (fk_col, ref_table, ref_col) in fk_rows
            ]

            schema["tables"].append(
                {
                    "name": table_name,
                    "columns": columns,
                    "primary_key": primary_key,
                    "foreign_keys": foreign_keys,
                }
            )

    return schema
