# scripts/02_build_index.py
from __future__ import annotations
import argparse
from pathlib import Path
from src.rag.build_index import build_domain_index


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--domain", type=str, default=None, help="e.g. chinook")
    ap.add_argument("--all", action="store_true", help="build for all schemas in data/schemas/")
    ap.add_argument("--schemas_dir", type=str, default="data/schemas")
    ap.add_argument("--persist_dir", type=str, default="data/chroma")
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    schemas_dir = Path(args.schemas_dir)
    if args.all:
        for p in sorted(schemas_dir.glob("*.json")):
            domain = p.stem
            print(f"[index] building domain={domain} schema={p}")
            build_domain_index(domain=domain, schema_path=p, persist_dir=args.persist_dir, reset=args.reset)
        return

    if not args.domain:
        raise SystemExit("Provide --domain chinook OR use --all")

    schema_path = schemas_dir / f"{args.domain}.json"
    if not schema_path.exists():
        raise SystemExit(f"Schema not found: {schema_path}")

    print(f"[index] building domain={args.domain} schema={schema_path}")
    build_domain_index(domain=args.domain, schema_path=schema_path, persist_dir=args.persist_dir, reset=args.reset)


if __name__ == "__main__":
    main()
