# spaces.Dockerfile — HF Spaces deployment
# Streamlit app in Docker SDK mode (port 7860, SQLite, multi-LLM)
FROM python:3.12-slim

# System deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc curl wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONPATH=/app
ENV USE_SQLITE=true
ENV CHROMA_DIR=data/chroma
ENV SQL_MAX_ROWS=200

# Install Python deps (SQLite only — no psycopg2)
COPY spaces_requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source
COPY . .

# Build SQLite databases that can auto-download (NYC 311 + Synthea only)
# Olist needs KAGGLE_USERNAME + KAGGLE_KEY, which are HF Spaces *runtime* secrets
# (not available during Docker build). The app builds Olist on first boot if present.
RUN mkdir -p data/sqlite && \
    python scripts/build_databases.py --only nyc311 synthea && \
    echo "✅ SQLite databases built (NYC 311 + Synthea)"

# Build ChromaDB vector index (~60-120s)
RUN python scripts/02_build_index.py --all && \
    echo "✅ ChromaDB index built"

# HF Spaces requires port 7860
EXPOSE 7860

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:7860/_stcore/health || exit 1

CMD ["streamlit", "run", "src/app/app.py", \
     "--server.port=7860", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--server.fileWatcherType=none", \
     "--browser.gatherUsageStats=false"]
