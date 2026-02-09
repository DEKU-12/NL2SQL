#!/usr/bin/env bash
set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

export PYTHONPATH=.

python - <<'PY'
from src.config.load_config import load_domains_config
from src.db.execute import run_query

domains = load_domains_config()
print("Checking domains:", list(domains.keys()))

for d in domains.keys():
    r = run_query(d, "SELECT 1 as ok")
    assert r.rows[0][0] == 1
    print(f"✅ {d}: OK")

print("✅ Healthcheck passed for all domains.")
PY
