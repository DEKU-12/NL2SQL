from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_env() -> None:
    """
    Load .env from project root safely (no find_dotenv).
    Prevents AssertionError issues on some setups.
    """
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        load_dotenv()


def load_domains_config() -> Dict[str, Any]:
    cfg_path = PROJECT_ROOT / "config" / "domains.yaml"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Missing config file: {cfg_path}")

    with cfg_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if not isinstance(cfg, dict) or "domains" not in cfg:
        raise ValueError("domains.yaml must contain top-level key: domains")

    return cfg["domains"]


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    user: str
    password: str
    max_rows: int


def get_postgres_settings() -> PostgresSettings:
    load_env()
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    max_rows = int(os.getenv("SQL_MAX_ROWS", "200"))
    return PostgresSettings(host=host, port=port, user=user, password=password, max_rows=max_rows)
