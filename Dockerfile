# Dockerfile — NL→SQL Copilot (Streamlit app)
FROM python:3.12-slim

# System deps: libpq for psycopg2, gcc for some wheels
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONPATH=/app

# Install Python deps first (layer cache friendly)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source
COPY . .

# Make entrypoint executable
RUN chmod +x scripts/entrypoint.sh

# Streamlit port
EXPOSE 8501

# Health check — hits the Streamlit health endpoint
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["scripts/entrypoint.sh"]
