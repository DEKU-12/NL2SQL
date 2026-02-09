from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.config.load_config import load_domains_config
from src.db.schema_extract import extract_schema

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--domain", type=str, default=None, help="Extract schema for one domain")
    ap.add_argument("--all", action="store_true", help="Extract schema for all domains")
    args = ap.parse_args()

    domains = load_domains_config()

    if args.all:
        targets = list(domains.keys())
    elif args.domain:
        if args.domain not in domains:
            raise SystemExit(f"Unknown domain '{args.domain}'. Available: {list(domains.keys())}")
        targets = [args.domain]
    else:
        raise SystemExit("Provide --domain <name> or --all")

    for d in targets:
        out_rel = domains[d]["schema_out"]
        out_path = PROJECT_ROOT / out_rel
        out_path.parent.mkdir(parents=True, exist_ok=True)

        schema = extract_schema(d)
        out_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")
        print(f"✅ Wrote schema: {out_path}")

if __name__ == "__main__":
    main()
