from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.config.load_config import get_postgres_settings, load_domains_config


def get_engine(domain: str) -> Engine:
    domains = load_domains_config()
    if domain not in domains:
        raise KeyError(f"Unknown domain '{domain}'. Available: {list(domains.keys())}")

    settings = get_postgres_settings()
    dbname = domains[domain]["dbname"]

    url = f"postgresql+psycopg2://{settings.user}:{settings.password}@{settings.host}:{settings.port}/{dbname}"
    return create_engine(url, pool_pre_ping=True, future=True)
