#!/usr/bin/env python3
"""
scripts/build_databases.py
==========================
NL2SQL Dataset Upgrade — Steps 1 & 2

Downloads (where possible) and builds 3 SQLite databases:

  1. nyc_311.db          — NYC 311 Government Service Requests   (auto-download)
  2. olist_ecommerce.db  — Brazilian E-Commerce (Olist)          (Kaggle API or manual)
  3. synthea_patients.db — Synthea Synthetic Patient Records      (auto-download)

Usage
-----
    # Build all databases:
    python scripts/build_databases.py

    # Build only specific databases:
    python scripts/build_databases.py --only olist
    python scripts/build_databases.py --only nyc311 synthea

    For Olist auto-download: set KAGGLE_USERNAME + KAGGLE_KEY environment variables.
    Without credentials, the script prints manual download instructions.

Requirements
------------
    pip install pandas requests kaggle
    (pandas and requests are already in requirements.txt)

Output
------
    data/sqlite/nyc_311.db
    data/sqlite/olist_ecommerce.db
    data/sqlite/synthea_patients.db
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import zipfile
import sqlite3
import subprocess
import textwrap
from pathlib import Path

import requests
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
#  DIRECTORY LAYOUT
# ─────────────────────────────────────────────────────────────────────────────

# Resolve relative to this script so it works from any cwd
BASE_DIR  = Path(__file__).resolve().parent.parent   # project root
DATA_RAW  = BASE_DIR / "data" / "raw"
DATA_DB   = BASE_DIR / "data" / "sqlite"             # matches SQLITE_DIR in sqlite_connect.py

DATA_RAW.mkdir(parents=True, exist_ok=True)
DATA_DB.mkdir(parents=True, exist_ok=True)

# Output database paths
NYC_DB_PATH     = DATA_DB / "nyc_311.db"
OLIST_DB_PATH   = DATA_DB / "olist_ecommerce.db"
SYNTHEA_DB_PATH = DATA_DB / "synthea_patients.db"

# Raw data staging areas
NYC_RAW_CSV  = DATA_RAW / "nyc_311_raw.csv"
OLIST_DIR    = DATA_RAW / "olist"
SYNTHEA_ZIP  = DATA_RAW / "synthea_csv.zip"
SYNTHEA_DIR  = DATA_RAW / "synthea"

# ─────────────────────────────────────────────────────────────────────────────
#  DOWNLOAD SOURCES
# ─────────────────────────────────────────────────────────────────────────────

# NYC 311 — Socrata Open Data API (no auth required)
NYC_SOCRATA_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
NYC_ROW_LIMIT   = 500_000

# Synthea — 10k COVID-19 CSV sample hosted on GitHub (no auth required, ~50 MB)
# To use the full 100k sample instead:
#   1. Visit https://synthea.mitre.org/downloads
#   2. Download the CSV zip manually and place it at: data/raw/synthea_csv.zip
#   The script will use whatever zip is already at that path.
SYNTHEA_CSV_URL = (
    "https://synthetichealth.github.io/synthea-sample-data/"
    "downloads/10k_synthea_covid19_csv.zip"
)


# ─────────────────────────────────────────────────────────────────────────────
#  SHARED UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _bar(title: str) -> None:
    line = "═" * 62
    print(f"\n{line}\n  {title}\n{line}")


def _ok(msg: str) -> None:
    print(f"  ✅ {msg}")


def _warn(msg: str) -> None:
    print(f"  ⚠️  {msg}")


def _info(msg: str) -> None:
    print(f"  ℹ  {msg}")


def download_file(url: str, dest: Path, label: str) -> None:
    """Stream-download *url* to *dest* with a progress line."""
    print(f"  ↓  Downloading {label} ...")
    with requests.get(url, stream=True, timeout=180) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        downloaded = 0
        chunk_size = 512 * 1024  # 512 KB
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    fh.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded / total * 100
                        mb_done = downloaded / 1_048_576
                        mb_total = total / 1_048_576
                        print(
                            f"\r     {pct:5.1f}%  "
                            f"{mb_done:.1f} / {mb_total:.1f} MB",
                            end="",
                            flush=True,
                        )
        print(f"\r     Done — {downloaded / 1_048_576:.1f} MB → {dest.name}         ")


def clean_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase column names; replace spaces/hyphens/slashes with underscores."""
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"[\s\-/]+", "_", regex=True)
          .str.replace(r"[^\w]", "", regex=True)
    )
    return df


def row_count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]


def drop_and_create(conn: sqlite3.Connection, tables: list[str], ddl: str) -> None:
    """Drop listed tables then execute DDL."""
    for t in tables:
        conn.execute(f"DROP TABLE IF EXISTS [{t}]")
    conn.commit()
    for stmt in ddl.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    conn.commit()


# ═════════════════════════════════════════════════════════════════════════════
#  DATABASE 1 — NYC 311 GOVERNMENT DATA
# ═════════════════════════════════════════════════════════════════════════════

NYC_DDL = """
CREATE TABLE nyc_311 (
    unique_key                     INTEGER PRIMARY KEY,
    created_date                   TEXT,
    closed_date                    TEXT,
    agency                         TEXT,
    agency_name                    TEXT,
    complaint_type                 TEXT,
    descriptor                     TEXT,
    location_type                  TEXT,
    borough                        TEXT,
    city                           TEXT,
    status                         TEXT,
    resolution_description         TEXT,
    latitude                       REAL,
    longitude                      REAL,
    resolution_action_updated_date TEXT
)
"""

# Canonical target columns (must match DDL above)
NYC_TARGET_COLS = [
    "unique_key",
    "created_date",
    "closed_date",
    "agency",
    "agency_name",
    "complaint_type",
    "descriptor",
    "location_type",
    "borough",
    "city",
    "status",
    "resolution_description",
    "latitude",
    "longitude",
    "resolution_action_updated_date",
]


def _download_nyc_311() -> None:
    if NYC_RAW_CSV.exists():
        _info(f"Raw CSV already present ({NYC_RAW_CSV.name}) — skipping download.")
        return

    print(f"  ↓  Querying Socrata API for {NYC_ROW_LIMIT:,} rows (2022–2024) ...")
    params = {
        "$limit":  NYC_ROW_LIMIT,
        "$where":  "created_date >= '2022-01-01T00:00:00'",
        "$order":  "created_date DESC",
    }
    # Build the URL manually so it is visible in error messages
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{NYC_SOCRATA_URL}?{qs}"
    download_file(url, NYC_RAW_CSV, f"NYC 311 ({NYC_ROW_LIMIT:,} rows, 2022-2024)")


def build_nyc_311() -> None:
    _bar("DATABASE 1 — NYC 311 Government Data")

    if NYC_DB_PATH.exists() and NYC_DB_PATH.stat().st_size > 1_000_000:
        _ok(f"nyc_311.db already exists ({NYC_DB_PATH.stat().st_size // 1_048_576} MB) — skipping rebuild.")
        return

    _download_nyc_311()

    print("  ⚙️  Reading CSV ...")
    df = pd.read_csv(NYC_RAW_CSV, low_memory=False)
    df = clean_df_columns(df)

    # Keep only schema columns that actually exist in the download
    present = [c for c in NYC_TARGET_COLS if c in df.columns]
    missing = [c for c in NYC_TARGET_COLS if c not in df.columns]
    if missing:
        _warn(f"Columns not found in CSV (will be NULL): {missing}")

    df = df.reindex(columns=present)

    # Coerce types
    df["unique_key"] = pd.to_numeric(df["unique_key"], errors="coerce")
    if "latitude" in df.columns:
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    if "longitude" in df.columns:
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    # Primary key must not be null
    df = df.dropna(subset=["unique_key"])
    df["unique_key"] = df["unique_key"].astype(int)
    df = df.drop_duplicates(subset=["unique_key"])

    print(f"  📊 Rows to load: {len(df):,}")

    with sqlite3.connect(NYC_DB_PATH) as conn:
        drop_and_create(conn, ["nyc_311"], NYC_DDL)
        df.to_sql("nyc_311", conn, if_exists="append", index=False)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_311_borough        ON nyc_311(borough)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_311_complaint_type ON nyc_311(complaint_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_311_created_date   ON nyc_311(created_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_311_agency         ON nyc_311(agency)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_311_status         ON nyc_311(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_311_descriptor     ON nyc_311(descriptor)")
        conn.commit()

        n = row_count(conn, "nyc_311")

    _ok(f"nyc_311.db ready — {n:,} rows → data/sqlite/nyc_311.db")


# ═════════════════════════════════════════════════════════════════════════════
#  DATABASE 2 — BRAZILIAN E-COMMERCE (OLIST)
# ═════════════════════════════════════════════════════════════════════════════

OLIST_INSTRUCTIONS = textwrap.dedent(f"""
    ┌──────────────────────────────────────────────────────────────┐
    │              OLIST — MANUAL DOWNLOAD REQUIRED                │
    ├──────────────────────────────────────────────────────────────┤
    │  Kaggle requires a free account (no payment needed).         │
    │                                                              │
    │  1. Visit:                                                   │
    │     https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
    │  2. Click "Download" (log in if prompted).                   │
    │  3. Extract the zip — you'll get a folder of CSVs.           │
    │  4. Copy ALL .csv files into:                                │
    │     {str(OLIST_DIR):<60}│
    │                                                              │
    │  Required files:                                             │
    │    • olist_orders_dataset.csv                                │
    │    • olist_order_items_dataset.csv                           │
    │    • olist_order_payments_dataset.csv                        │
    │    • olist_order_reviews_dataset.csv                         │
    │    • olist_customers_dataset.csv                             │
    │    • olist_products_dataset.csv                              │
    │    • olist_sellers_dataset.csv                               │
    │    • olist_geolocation_dataset.csv                           │
    │                                                              │
    │  Re-run this script after placing the files.                 │
    └──────────────────────────────────────────────────────────────┘
""")

OLIST_DDL = """
CREATE TABLE orders (
    order_id                       TEXT PRIMARY KEY,
    customer_id                    TEXT,
    order_status                   TEXT,
    order_purchase_timestamp       TEXT,
    order_approved_at              TEXT,
    order_delivered_carrier_date   TEXT,
    order_delivered_customer_date  TEXT,
    order_estimated_delivery_date  TEXT
);
CREATE TABLE order_items (
    order_id            TEXT,
    order_item_id       INTEGER,
    product_id          TEXT,
    seller_id           TEXT,
    shipping_limit_date TEXT,
    price               REAL,
    freight_value       REAL
);
CREATE TABLE order_payments (
    order_id             TEXT,
    payment_sequential   INTEGER,
    payment_type         TEXT,
    payment_installments INTEGER,
    payment_value        REAL
);
CREATE TABLE order_reviews (
    review_id               TEXT PRIMARY KEY,
    order_id                TEXT,
    review_score            INTEGER,
    review_comment_title    TEXT,
    review_comment_message  TEXT,
    review_creation_date    TEXT,
    review_answer_timestamp TEXT
);
CREATE TABLE customers (
    customer_id              TEXT PRIMARY KEY,
    customer_unique_id       TEXT,
    customer_zip_code_prefix TEXT,
    customer_city            TEXT,
    customer_state           TEXT
);
CREATE TABLE products (
    product_id                 TEXT PRIMARY KEY,
    product_category_name      TEXT,
    product_name_length        INTEGER,
    product_description_length INTEGER,
    product_photos_qty         INTEGER,
    product_weight_g           REAL,
    product_length_cm          REAL,
    product_height_cm          REAL,
    product_width_cm           REAL
);
CREATE TABLE sellers (
    seller_id               TEXT PRIMARY KEY,
    seller_zip_code_prefix  TEXT,
    seller_city             TEXT,
    seller_state            TEXT
);
CREATE TABLE geolocation (
    geolocation_zip_code_prefix TEXT,
    geolocation_lat             REAL,
    geolocation_lng             REAL,
    geolocation_city            TEXT,
    geolocation_state           TEXT
)
"""

# CSV filename → (sqlite table name, [expected columns], dedup_key or None)
# dedup_key: column(s) to deduplicate on before INSERT (avoids UNIQUE violations).
# The Olist dataset is known to contain duplicate review_ids, order_ids, etc.
OLIST_FILE_MAP: list[tuple[str, str, list[str], list[str] | None]] = [
    (
        "olist_orders_dataset.csv",
        "orders",
        [
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        ["order_id"],
    ),
    (
        "olist_order_items_dataset.csv",
        "order_items",
        [
            "order_id", "order_item_id", "product_id", "seller_id",
            "shipping_limit_date", "price", "freight_value",
        ],
        ["order_id", "order_item_id"],
    ),
    (
        "olist_order_payments_dataset.csv",
        "order_payments",
        [
            "order_id", "payment_sequential", "payment_type",
            "payment_installments", "payment_value",
        ],
        ["order_id", "payment_sequential"],
    ),
    (
        "olist_order_reviews_dataset.csv",
        "order_reviews",
        [
            "review_id", "order_id", "review_score",
            "review_comment_title", "review_comment_message",
            "review_creation_date", "review_answer_timestamp",
        ],
        ["review_id"],   # dataset has ~1k duplicate review_ids — keep first occurrence
    ),
    (
        "olist_customers_dataset.csv",
        "customers",
        [
            "customer_id", "customer_unique_id",
            "customer_zip_code_prefix", "customer_city", "customer_state",
        ],
        ["customer_id"],
    ),
    (
        "olist_products_dataset.csv",
        "products",
        [
            "product_id", "product_category_name",
            "product_name_length", "product_description_length",
            "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm",
        ],
        ["product_id"],
    ),
    (
        "olist_sellers_dataset.csv",
        "sellers",
        [
            "seller_id", "seller_zip_code_prefix",
            "seller_city", "seller_state",
        ],
        ["seller_id"],
    ),
    (
        "olist_geolocation_dataset.csv",
        "geolocation",
        [
            "geolocation_zip_code_prefix", "geolocation_lat",
            "geolocation_lng", "geolocation_city", "geolocation_state",
        ],
        None,  # no PK — keep all rows
    ),
]


def _find_olist_csv(name: str) -> Path | None:
    """Recursively search OLIST_DIR for a file named *name* (handles Kaggle subdirs)."""
    matches = sorted(OLIST_DIR.rglob(name))
    return matches[0] if matches else None


def _download_olist_kaggle() -> bool:
    """
    Download Olist CSVs using the Kaggle API if credentials are available.

    Supports both auth styles:
      New-style:  KAGGLE_API_TOKEN=KGAT_...   (single token, kaggle>=1.6)
      Old-style:  KAGGLE_USERNAME + KAGGLE_KEY (username + api key from kaggle.json)

    Returns True if the download succeeded.
    """
    api_token = os.getenv("KAGGLE_API_TOKEN")           # new single-token style
    username  = os.getenv("KAGGLE_USERNAME")
    key       = os.getenv("KAGGLE_KEY") or os.getenv("KAGGLE_API_KEY")

    if not api_token and not (username and key):
        return False

    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(exist_ok=True)

    if api_token:
        _info("KAGGLE_API_TOKEN detected — attempting auto-download ...")
        # New-style: write token to access_token file (kaggle>=1.6)
        token_file = kaggle_dir / "access_token"
        token_file.write_text(api_token)
        token_file.chmod(0o600)
    else:
        _info("KAGGLE_USERNAME + KAGGLE_KEY detected — attempting auto-download ...")
        # Old-style: write kaggle.json
        kaggle_json = kaggle_dir / "kaggle.json"
        kaggle_json.write_text(json.dumps({"username": username, "key": key}))
        kaggle_json.chmod(0o600)

    env = {**os.environ}
    if api_token:
        env["KAGGLE_API_TOKEN"] = api_token

    print("  ↓  Running: kaggle datasets download -d olistbr/brazilian-ecommerce ...")
    result = subprocess.run(
        [sys.executable, "-m", "kaggle", "datasets", "download",
         "-d", "olistbr/brazilian-ecommerce",
         "-p", str(OLIST_DIR), "--unzip"],
        capture_output=True, text=True, timeout=300, env=env,
    )
    if result.returncode != 0:
        _warn(f"Kaggle download failed:\n{result.stderr[:400]}")
        return False

    _ok("Olist CSVs downloaded from Kaggle.")
    return True


def build_olist() -> bool:
    _bar("DATABASE 2 — Brazilian E-Commerce (Olist)")

    if OLIST_DB_PATH.exists() and OLIST_DB_PATH.stat().st_size > 1_000_000:
        _ok(f"olist_ecommerce.db already exists ({OLIST_DB_PATH.stat().st_size // 1_048_576} MB) — skipping rebuild.")
        return True

    OLIST_DIR.mkdir(parents=True, exist_ok=True)

    # Check for missing files — try Kaggle API if credentials present
    missing_files = [
        csv_name
        for csv_name, _, _, _ in OLIST_FILE_MAP
        if _find_olist_csv(csv_name) is None
    ]
    if missing_files:
        if not _download_olist_kaggle():
            print(OLIST_INSTRUCTIONS)
            print("  Missing files:")
            for f in missing_files:
                print(f"    • {f}")
            print("\n  ⏭️  Skipping Olist build. Set KAGGLE_USERNAME + KAGGLE_KEY or place CSV files manually.\n")
            return False
        # Re-check after download
        missing_files = [
            csv_name
            for csv_name, _, _, _ in OLIST_FILE_MAP
            if _find_olist_csv(csv_name) is None
        ]
        if missing_files:
            _warn(f"Still missing after Kaggle download: {missing_files}")
            return False

    print("  ⚙️  Building olist_ecommerce.db ...")

    all_tables = [t for _, t, _, _ in OLIST_FILE_MAP]
    with sqlite3.connect(OLIST_DB_PATH) as conn:
        drop_and_create(conn, all_tables, OLIST_DDL)

        totals: dict[str, int] = {}
        for csv_name, table, target_cols, dedup_key in OLIST_FILE_MAP:
            path = _find_olist_csv(csv_name) or (OLIST_DIR / csv_name)
            print(f"    Loading {csv_name:<45} → {table} ...", end=" ", flush=True)

            df = pd.read_csv(path, low_memory=False)
            df = clean_df_columns(df)

            # Fix Kaggle's well-known typos in the products file
            df = df.rename(columns={
                "product_name_lenght":        "product_name_length",
                "product_description_lenght": "product_description_length",
            })

            # Keep only columns that exist in the schema
            keep = [c for c in target_cols if c in df.columns]
            df = df[keep]

            # Deduplicate on primary key to avoid UNIQUE constraint violations
            # (the Olist dataset contains duplicate review_ids and other duplicates)
            if dedup_key:
                valid_keys = [k for k in dedup_key if k in df.columns]
                if valid_keys:
                    before = len(df)
                    df = df.drop_duplicates(subset=valid_keys, keep="first")
                    dropped = before - len(df)
                    if dropped:
                        print(f"(dropped {dropped} dupes) ", end="", flush=True)

            df.to_sql(table, conn, if_exists="append", index=False)
            n = row_count(conn, table)
            totals[table] = n
            print(f"{n:,} rows")

        # Indexes optimised for benchmark queries
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olist_orders_customer   ON orders(customer_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olist_orders_status     ON orders(order_status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olist_orders_purchase   ON orders(order_purchase_timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olist_items_order       ON order_items(order_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olist_items_product     ON order_items(product_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olist_items_seller      ON order_items(seller_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olist_payments_order    ON order_payments(order_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olist_payments_type     ON order_payments(payment_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olist_reviews_order     ON order_reviews(order_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olist_reviews_score     ON order_reviews(review_score)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olist_customers_state   ON customers(customer_state)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olist_customers_city    ON customers(customer_city)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olist_products_category ON products(product_category_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olist_sellers_state     ON sellers(seller_state)")
        conn.commit()

    _ok(f"olist_ecommerce.db ready → data/sqlite/olist_ecommerce.db")
    for t, n in totals.items():
        print(f"     {t:<25} {n:>10,} rows")

    return True


# ═════════════════════════════════════════════════════════════════════════════
#  DATABASE 3 — SYNTHEA SYNTHETIC PATIENT RECORDS
# ═════════════════════════════════════════════════════════════════════════════

SYNTHEA_DDL = """
CREATE TABLE patients (
    id                  TEXT PRIMARY KEY,
    birthdate           TEXT,
    deathdate           TEXT,
    ssn                 TEXT,
    first               TEXT,
    last                TEXT,
    race                TEXT,
    ethnicity           TEXT,
    gender              TEXT,
    birthplace          TEXT,
    address             TEXT,
    city                TEXT,
    state               TEXT,
    zip                 TEXT,
    lat                 REAL,
    lon                 REAL,
    healthcare_expenses REAL,
    healthcare_coverage REAL,
    income              INTEGER
);
CREATE TABLE encounters (
    id                  TEXT PRIMARY KEY,
    start               TEXT,
    stop                TEXT,
    patient             TEXT,
    organization        TEXT,
    provider            TEXT,
    payer               TEXT,
    encounterclass      TEXT,
    code                TEXT,
    description         TEXT,
    base_encounter_cost REAL,
    total_claim_cost    REAL,
    payer_coverage      REAL,
    reasoncode          TEXT,
    reasondescription   TEXT
);
CREATE TABLE conditions (
    start       TEXT,
    stop        TEXT,
    patient     TEXT,
    encounter   TEXT,
    code        TEXT,
    description TEXT
);
CREATE TABLE medications (
    start             TEXT,
    stop              TEXT,
    patient           TEXT,
    payer             TEXT,
    encounter         TEXT,
    code              TEXT,
    description       TEXT,
    base_cost         REAL,
    payer_coverage    REAL,
    dispenses         INTEGER,
    totalcost         REAL,
    reasoncode        TEXT,
    reasondescription TEXT
);
CREATE TABLE procedures (
    start             TEXT,
    stop              TEXT,
    patient           TEXT,
    encounter         TEXT,
    code              TEXT,
    description       TEXT,
    base_cost         REAL,
    reasoncode        TEXT,
    reasondescription TEXT
);
CREATE TABLE allergies (
    start       TEXT,
    stop        TEXT,
    patient     TEXT,
    encounter   TEXT,
    code        TEXT,
    description TEXT,
    type        TEXT,
    category    TEXT,
    reaction1   TEXT,
    severity1   TEXT
)
"""

SYNTHEA_TABLE_ORDER = [
    "patients",
    "encounters",
    "conditions",
    "medications",
    "procedures",
    "allergies",
]

# Column renames applied after clean_df_columns()
SYNTHEA_COL_RENAMES = {
    "long":  "lon",
    "lng":   "lon",
}

# Allowed columns per table (extras are dropped to avoid sqlite type errors)
SYNTHEA_SCHEMA_COLS: dict[str, list[str]] = {
    "patients": [
        "id", "birthdate", "deathdate", "ssn", "first", "last", "race",
        "ethnicity", "gender", "birthplace", "address", "city", "state",
        "zip", "lat", "lon", "healthcare_expenses", "healthcare_coverage", "income",
    ],
    "encounters": [
        "id", "start", "stop", "patient", "organization", "provider", "payer",
        "encounterclass", "code", "description", "base_encounter_cost",
        "total_claim_cost", "payer_coverage", "reasoncode", "reasondescription",
    ],
    "conditions": ["start", "stop", "patient", "encounter", "code", "description"],
    "medications": [
        "start", "stop", "patient", "payer", "encounter", "code", "description",
        "base_cost", "payer_coverage", "dispenses", "totalcost",
        "reasoncode", "reasondescription",
    ],
    "procedures": [
        "start", "stop", "patient", "encounter", "code", "description",
        "base_cost", "reasoncode", "reasondescription",
    ],
    "allergies": [
        "start", "stop", "patient", "encounter", "code", "description",
        "type", "category", "reaction1", "severity1",
    ],
}


def _download_synthea() -> None:
    if SYNTHEA_ZIP.exists():
        _info(f"Synthea zip already present ({SYNTHEA_ZIP.name}) — skipping download.")
        _info("To use the 100k sample, replace data/raw/synthea_csv.zip with the zip")
        _info("from https://synthea.mitre.org/downloads and re-run.")
        return
    download_file(SYNTHEA_CSV_URL, SYNTHEA_ZIP, "Synthea 10k CSV sample (~50 MB)")


def _extract_synthea() -> None:
    if SYNTHEA_DIR.exists() and any(SYNTHEA_DIR.rglob("patients.csv")):
        _info(f"Synthea CSVs already extracted to {SYNTHEA_DIR.name}/")
        return
    SYNTHEA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  📦 Extracting {SYNTHEA_ZIP.name} ...")
    with zipfile.ZipFile(SYNTHEA_ZIP, "r") as z:
        z.extractall(SYNTHEA_DIR)
    _ok(f"Extracted to {SYNTHEA_DIR.name}/")


def _find_synthea_csv(name: str) -> Path | None:
    """Recursively search SYNTHEA_DIR for a file named *name*."""
    matches = sorted(SYNTHEA_DIR.rglob(name))
    return matches[0] if matches else None


def build_synthea() -> None:
    _bar("DATABASE 3 — Synthea Synthetic Patient Records")

    if SYNTHEA_DB_PATH.exists() and SYNTHEA_DB_PATH.stat().st_size > 1_000_000:
        _ok(f"synthea_patients.db already exists ({SYNTHEA_DB_PATH.stat().st_size // 1_048_576} MB) — skipping rebuild.")
        return

    _download_synthea()
    _extract_synthea()

    print("  ⚙️  Building synthea_patients.db ...")

    with sqlite3.connect(SYNTHEA_DB_PATH) as conn:
        drop_and_create(conn, SYNTHEA_TABLE_ORDER, SYNTHEA_DDL)

        totals: dict[str, int] = {}
        for table in SYNTHEA_TABLE_ORDER:
            csv_path = _find_synthea_csv(f"{table}.csv")
            if csv_path is None:
                _warn(f"{table}.csv not found — table will be empty.")
                totals[table] = 0
                continue

            print(f"    Loading {table+'.csv':<20} → {table} ...", end=" ", flush=True)
            df = pd.read_csv(csv_path, low_memory=False)
            df = clean_df_columns(df)
            df = df.rename(columns=SYNTHEA_COL_RENAMES)

            keep = [c for c in SYNTHEA_SCHEMA_COLS[table] if c in df.columns]
            df = df[keep]

            df.to_sql(table, conn, if_exists="append", index=False)
            n = row_count(conn, table)
            totals[table] = n
            print(f"{n:,} rows")

        # Indexes for benchmark queries
        conn.execute("CREATE INDEX IF NOT EXISTS idx_syn_enc_patient     ON encounters(patient)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_syn_enc_class       ON encounters(encounterclass)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_syn_enc_start       ON encounters(start)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_syn_cond_patient    ON conditions(patient)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_syn_cond_desc       ON conditions(description)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_syn_med_patient     ON medications(patient)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_syn_med_desc        ON medications(description)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_syn_proc_patient    ON procedures(patient)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_syn_proc_desc       ON procedures(description)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_syn_allergy_patient ON allergies(patient)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_syn_pat_gender      ON patients(gender)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_syn_pat_state       ON patients(state)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_syn_pat_race        ON patients(race)")
        conn.commit()

    _ok(f"synthea_patients.db ready → data/sqlite/synthea_patients.db")
    for t, n in totals.items():
        print(f"     {t:<20} {n:>10,} rows")


# ═════════════════════════════════════════════════════════════════════════════
#  FINAL VERIFICATION
# ═════════════════════════════════════════════════════════════════════════════

def verify_all() -> None:
    _bar("VERIFICATION SUMMARY")

    checks: list[tuple[Path, list[tuple[str, int]]]] = [
        (NYC_DB_PATH, [
            ("nyc_311", 10_000),  # sanity floor — expect ~500k
        ]),
        (OLIST_DB_PATH, [
            ("orders", 1), ("order_items", 1), ("order_payments", 1),
            ("order_reviews", 1), ("customers", 1), ("products", 1), ("sellers", 1),
        ]),
        (SYNTHEA_DB_PATH, [
            ("patients", 1), ("encounters", 1), ("conditions", 1),
            ("medications", 1), ("procedures", 1),
        ]),
    ]

    all_ok = True
    for db_path, tables in checks:
        if not db_path.exists():
            _warn(f"{db_path.name} — NOT BUILT (skipped or failed)")
            all_ok = False
            continue
        with sqlite3.connect(db_path) as conn:
            for table, min_rows in tables:
                try:
                    n = row_count(conn, table)
                    ok = n >= min_rows
                    icon = "✅" if ok else "⚠️ "
                    if not ok:
                        all_ok = False
                    print(f"  {icon}  {db_path.name:<32}  {table:<25}  {n:>10,} rows")
                except Exception as exc:
                    print(f"  ✗   {db_path.name}.{table} — ERROR: {exc}")
                    all_ok = False

    print()
    if all_ok:
        print("  🎉 All databases built. Ready for Step 3 (update config).")
    else:
        print("  ⚠️  One or more checks failed — see above before continuing to Step 3.")


# ═════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

def main() -> None:
    ap = argparse.ArgumentParser(description="Build NL2SQL SQLite databases")
    ap.add_argument(
        "--only", nargs="+",
        choices=["nyc311", "olist", "synthea"],
        metavar="DB",
        help="Build only specific databases (nyc311, olist, synthea). Default: all.",
    )
    args = ap.parse_args()
    only = set(args.only) if args.only else {"nyc311", "olist", "synthea"}

    t0 = time.time()

    print("\n" + "═" * 62)
    print("  NL2SQL Dataset Upgrade — build_databases.py")
    print(f"  Building: {', '.join(sorted(only))}")
    print("  Output:   data/sqlite/")
    print("═" * 62)

    if "nyc311" in only:
        build_nyc_311()
    if "olist" in only:
        ok = build_olist()
        if not ok:
            print("\n  ✗  Olist build failed — exiting with code 1", file=sys.stderr)
            sys.exit(1)
    if "synthea" in only:
        build_synthea()

    verify_all()

    elapsed = time.time() - t0
    print(f"  ⏱  Total time: {elapsed:.1f}s\n")
    print("═" * 62 + "\n")


if __name__ == "__main__":
    main()
