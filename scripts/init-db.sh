#!/usr/bin/env bash
# scripts/init-db.sh
# Runs automatically inside the postgres container on first boot.
# Creates all three databases and loads the seed data.
set -e

echo "[init-db] Creating databases..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE chinook;
    CREATE DATABASE dvdrental;
    CREATE DATABASE northwind;
EOSQL

echo "[init-db] Loading chinook..."
# Strip DROP/CREATE DATABASE lines — we already created it above
grep -iv "DROP DATABASE\|CREATE DATABASE" /docker-sql/chinook_pg.sql | \
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d chinook

echo "[init-db] Loading northwind..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d northwind < /docker-sql/northwind.sql

echo "[init-db] Loading dvdrental (pg_restore)..."
pg_restore --username "$POSTGRES_USER" -d dvdrental /docker-sql/dvdrental.tar || true

echo "[init-db] All databases ready."
