# Movies Data Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue)
![dbt](https://img.shields.io/badge/dbt-1.7.4-orange)
![Airflow](https://img.shields.io/badge/Airflow-2.8.1-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-lightgrey)

End-to-end movie analytics pipeline that ingests raw movie data from Google Cloud Storage, transforms it through Bronze -> Silver -> Gold layers, and serves a PostgreSQL star schema to Power BI.

This project was built as a data engineering capstone and ends in an acquisition-planning dashboard in [movie_capstone.pbix](movie_capstone.pbix).

![System Architecture](docs/system_architecture.jpg)

## What This Project Does

- Ingests `movies_main.csv` and `movie_extended.csv` from GCS
- Loads raw files into PostgreSQL Bronze tables
- Cleans, types, deduplicates, and enriches records in Silver using Python and the TMDB API
- Builds a Gold star schema in dbt for reporting
- Powers a Power BI dashboard for catalog diagnostics and acquisition prioritization

## Tech Stack

| Tool | Role |
|------|------|
| Python + Pandas | Bronze and Silver processing |
| Pandera | Data validation |
| Airflow | Pipeline orchestration |
| PostgreSQL | Warehouse storage |
| dbt | Gold transformations and tests |
| Docker Compose | Local environment |
| Power BI Desktop | Final reporting layer |

## Data Model

| Layer | Purpose | Main Objects |
|-------|---------|--------------|
| Bronze | Raw ingested files | `movies_main`, `movie_extended` |
| Silver | Cleaned and enriched tables | `movies`, `movie_genres`, `production_companies`, `producing_countries`, `spoken_languages`, `movies_enriched` |
| Gold | BI-ready star schema | `fact_movies`, 4 bridge tables, 4 marts, `mart_yearly_trends` |

## Repository Layout

```text
data-engineering-capstone-project/
|-- dags/
|   `-- movie_pipeline_dag.py
|-- datasets/
|   |-- movies_main.csv
|   `-- movie_extended.csv
|-- dbt/
|   |-- dbt_project.yml
|   |-- profiles.yml
|   |-- macros/
|   |   |-- classify_budget.sql
|   |   `-- pct_of_total.sql
|   |-- models/
|   |   |-- staging/
|   |   |   |-- sources.yml
|   |   |   |-- schema.yml
|   |   |   |-- stg_movies.sql
|   |   |   |-- stg_movie_genres.sql
|   |   |   |-- stg_movie_companies.sql
|   |   |   |-- stg_movie_countries.sql
|   |   |   `-- stg_movie_languages.sql
|   |   |-- intermediate/
|   |   |   |-- schema.yml
|   |   |   |-- int_movie_financials.sql
|   |   |   `-- int_yearly_movie_trends.sql
|   |   `-- marts/
|   |       |-- schema.yml
|   |       |-- fact_movies.sql
|   |       |-- bridge_movie_genres.sql
|   |       |-- bridge_movie_companies.sql
|   |       |-- bridge_movie_countries.sql
|   |       |-- bridge_movie_languages.sql
|   |       |-- mart_genre_share.sql
|   |       |-- mart_language_share.sql
|   |       |-- mart_country_summary.sql
|   |       `-- mart_yearly_trends.sql
|   `-- tests/
|-- docker/
|   |-- airflow/
|   |   `-- Dockerfile
|   |-- dbt-model/
|   |   `-- Dockerfile
|   `-- pandas-worker/
|       `-- Dockerfile
|-- docs/
|   |-- 01_start_here_checklist.md
|   |-- 02_new_device_setup_and_pipeline_run.md
|   |-- 03_power_bi_refresh_and_dashboard_rebuild.md
|   |-- 04_validation_and_troubleshooting.md
|   `-- system_architecture.jpg
|-- logs/
|-- postgres/
|   `-- init.sh
|-- scripts/
|   |-- bronze/
|   |   |-- bronze_ddl.py
|   |   |-- bronze_load.py
|   |   `-- bronze_validate.py
|   `-- silver/
|       |-- silver_ddl.py
|       |-- silver_enrich.py
|       |-- silver_transform.py
|       `-- silver_validate.py
|-- secrets/
|   `-- gcs_key.json
|-- .env
|-- .gitattributes
|-- .gitignore
|-- docker-compose.yml
|-- movie_capstone.pbix
`-- README.md
```

## Start Here

The README is intentionally brief. Use these docs for full setup and rebuild steps:

1. [Start Here: New-Device Reproducibility Checklist](docs/01_start_here_checklist.md)
2. [New Device Setup and Pipeline Run](docs/02_new_device_setup_and_pipeline_run.md)
3. [Power BI Refresh and Dashboard Rebuild](docs/03_power_bi_refresh_and_dashboard_rebuild.md)
4. [Validation and Troubleshooting](docs/04_validation_and_troubleshooting.md)

## Quick Start

1. Add your GCS service account key to `secrets/gcs_key.json`.
2. Create a project `.env` with PostgreSQL, Airflow, and TMDB settings.
3. Start the stack:

```powershell
docker compose up -d --build
```

4. Open Airflow at `http://localhost:8080` and run the movie pipeline DAG.
5. Open `movie_capstone.pbix` and refresh the report against PostgreSQL on `localhost:5433`.

For exact environment variables, container checks, dbt commands, and Power BI rebuild steps, use the docs above.

## Pipeline Summary

### Bronze
- Reads raw CSVs from GCS
- Stores all fields as raw text
- Validates schema shape and row counts

### Silver
- Casts types and standardizes dates
- Deduplicates records
- Explodes genres, companies, countries, and languages into typed tables
- Enriches missing movie metadata through TMDB

### Gold
- Builds a reporting-friendly star schema in dbt
- Produces bridge tables and summary marts
- Supports Power BI analysis across genre, language, country, and yearly trends

## Key Outputs

- PostgreSQL `gold` schema for reporting
- dbt models and tests for the Gold layer
- Power BI dashboard in `movie_capstone.pbix`
- Reproducibility and troubleshooting runbooks in `docs/`

## Notes

- The tracked datasets in `datasets/` are reference copies only; the pipeline reads from GCS.
- `secrets/gcs_key.json` and `.env` are required locally and should not be committed.
- The Power BI file depends on the local PostgreSQL connection described in the docs.
