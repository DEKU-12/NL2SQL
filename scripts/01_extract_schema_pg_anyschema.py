from __future__ import annotations
import argparse, json, os
import psycopg2


def qall(cur, q, params=None):
    cur.execute(q, params or ())
    return cur.fetchall()


def extract_schema(conn):
    out = {"tables": []}

    cur = conn.cursor()
    tables = qall(cur, """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type='BASE TABLE'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
    """)

    for (sch, tname) in tables:
        cols = qall(conn.cursor(), """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position;
        """, (sch, tname))

        pk = qall(conn.cursor(), """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema=%s
              AND tc.table_name=%s
              AND tc.constraint_type='PRIMARY KEY'
            ORDER BY kcu.ordinal_position;
        """, (sch, tname))
        pk_cols = [c[0] for c in pk]

        fks = qall(conn.cursor(), """
            SELECT
              kcu.column_name,
              ccu.table_schema AS ref_schema,
              ccu.table_name AS ref_table,
              ccu.column_name AS ref_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.table_schema=%s
              AND tc.table_name=%s
              AND tc.constraint_type='FOREIGN KEY';
        """, (sch, tname))

        out["tables"].append({
            "table": f"{sch}.{tname}",
            "columns": [{"name": c, "type": dt} for (c, dt) in cols],
            "primary_key": pk_cols,
            "foreign_keys": [{"column": c, "ref_table": f"{rs}.{rt}", "ref_column": rc}
                             for (c, rs, rt, rc) in fks],
            "description": ""
        })

    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    user = os.getenv("PGUSER", "postgres")
    pwd  = os.getenv("PGPASSWORD", "postgres")

    conn = psycopg2.connect(host=host, port=port, dbname=args.db, user=user, password=pwd)
    schema_json = extract_schema(conn)
    conn.close()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump({"domain": args.db, **schema_json}, f, indent=2)

    print(f"Wrote {args.out} with {len(schema_json['tables'])} tables.")


if __name__ == "__main__":
    main()
