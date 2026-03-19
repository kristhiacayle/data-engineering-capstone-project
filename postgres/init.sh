#!/bin/bash
# Postgres Initialization Script
# Ito ay nagru-run ISANG BESES lang — sa unang startup ng Postgres container
# (kapag walang laman pa ang data directory)
#
# Dalawang ginagawa nito:
# 1. Gumawa ng tatlong schemas (bronze, silver, gold) sa movies_pipeline database
# 2. Gumawa ng airflow_metadata database para sa Airflow

set -e

# === Step 1: Gawa ng medallion schemas sa movies_pipeline database ===
# Tatlong schemas para sa medallion architecture:
# - bronze: raw data galing sa source (as-is)
# - silver: cleaned at validated data
# - gold: business-ready aggregations at models
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS bronze;
    CREATE SCHEMA IF NOT EXISTS silver;
    CREATE SCHEMA IF NOT EXISTS gold;
EOSQL

# === Step 2: Gawa ng airflow_metadata database ===
# Separate database para sa Airflow metadata (DAG runs, task instances, etc.)
# Kailangan mag-connect sa default 'postgres' database para makagawa ng bagong database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    CREATE DATABASE airflow_metadata OWNER $POSTGRES_USER;
EOSQL
