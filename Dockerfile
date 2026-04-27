# Dockerfile — NL→SQL Copilot (local SQLite mode)
FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc curl wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONPATH=/app
ENV USE_SQLITE=true
ENV CHROMA_DIR=data/chroma
ENV SQL_MAX_ROWS=200

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Build SQLite databases and ChromaDB index
RUN mkdir -p data/sqlite && \
    python scripts/build_databases.py && \
    python scripts/02_build_index.py --all

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "src/app/app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--server.fileWatcherType=none", \
     "--browser.gatherUsageStats=false"]
