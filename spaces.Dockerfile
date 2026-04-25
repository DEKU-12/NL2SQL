# spaces.Dockerfile — HF Spaces deployment
# Streamlit app in Docker SDK mode (port 7860, SQLite, OpenAI)
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

# Install Python deps (no psycopg2 — SQLite only)
COPY spaces_requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source
COPY . .

# Download pre-built SQLite databases from official public sources
RUN mkdir -p data/sqlite && \
    wget -q -O data/sqlite/chinook.db \
        "https://github.com/lerocha/chinook-database/releases/download/ChinookVersion_1.4.5/Chinook_Sqlite.sqlite" && \
    wget -q -O data/sqlite/northwind.db \
        "https://github.com/jpwhite3/northwind-SQLite3/raw/main/dist/northwind.db" && \
    echo "✅ SQLite databases downloaded"

# Build ChromaDB vector index (~60-120s, sentence-transformers model downloaded here)
RUN python scripts/02_build_index.py --all

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
