#!/usr/bin/env bash
# scripts/entrypoint.sh
export PYTHONPATH=/app
# Docker entrypoint for the app container.
# Builds the Chroma index on first boot, then starts Streamlit.
set -e

echo "[entrypoint] Waiting for Postgres to be ready..."
until python -c "
import psycopg2, os, sys
try:
    psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
        dbname='chinook'
    ).close()
    sys.exit(0)
except Exception:
    sys.exit(1)
" 2>/dev/null; do
    echo "[entrypoint] Postgres not ready yet — retrying in 2s..."
    sleep 2
done
echo "[entrypoint] Postgres is ready."

echo "[entrypoint] Building Chroma vector index (skipped if already built)..."
if [ ! -d "data/chroma" ] || [ -z "$(ls -A data/chroma 2>/dev/null)" ]; then
    echo "[entrypoint] Index not found — building now (this takes ~1-2 min on first run)..."
    PYTHONPATH=. python scripts/02_build_index.py --all
    echo "[entrypoint] Index built."
else
    echo "[entrypoint] Index already exists — skipping."
fi

echo "[entrypoint] Starting Streamlit on port 8501..."
exec streamlit run src/app/app.py \
    --server.port=8501 \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --server.fileWatcherType=none \
    --browser.gatherUsageStats=false
