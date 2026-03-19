"""
Silver Transform Script
Nagta-transform ng Bronze raw data papunta sa clean Silver tables.
Cast types, dedup, parse mixed date formats, merge TMDB enrichment,
explode genres at production companies sa separate tables.

Output tables:
- silver.movies — deduplicated, typed, enriched movies
- silver.movie_genres — one row per movie-genre pair
- silver.production_companies — one row per movie-company pair
"""

import os
import sys
import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text

# === Loguru Configuration ===
# Dalawang sinks: stdout para sa real-time monitoring, file para sa audit trail
logger.remove()
logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
logger.add("/logs/silver/silver.log", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", rotation="10 MB")

# === Output Tables ===
# Tatlong tables na sisimulan ng transform — ginagamit para sa TRUNCATE at verify
# DDL at COMMENTs ay nasa silver_ddl.py na — dito, table names lang ang kailangan
OUTPUT_TABLES = ["movies", "movie_genres", "production_companies"]


def get_engine():
    """Gawa ng SQLAlchemy engine gamit ang environment variables."""
    db_url = (
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    return create_engine(db_url)


def truncate_output_tables(engine):
    """TRUNCATE lahat ng output tables bago mag-insert — idempotent pattern."""
    for table_name in OUTPUT_TABLES:
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE silver.{table_name}"))
            conn.commit()
        logger.info(f"Na-truncate ang silver.{table_name}")


def load_source_data(engine):
    """
    Basahin ang source data mula sa bronze at silver.movies_enriched.
    Returns: df_main, df_extended, df_enriched
    """
    with engine.connect() as conn:
        df_main = pd.read_sql(text("SELECT * FROM bronze.movies_main"), conn)
        logger.info(f"Loaded bronze.movies_main: {len(df_main)} rows")

        df_extended = pd.read_sql(text("SELECT * FROM bronze.movie_extended"), conn)
        logger.info(f"Loaded bronze.movie_extended: {len(df_extended)} rows")

        df_enriched = pd.read_sql(text("SELECT * FROM silver.movies_enriched"), conn)
        logger.info(f"Loaded silver.movies_enriched: {len(df_enriched)} rows")

    return df_main, df_extended, df_enriched


def transform_movies(engine, df_main, df_enriched):
    """
    Transform bronze.movies_main → silver.movies.
    Dedup, parse dates, cast types, merge enrichment, trim titles.
    """
    logger.info("--- Simula ng silver.movies transform ---")

    # Step 4a: Deduplicate sa id column — keep first occurrence
    rows_before = len(df_main)
    df_main = df_main.drop_duplicates(subset=["id"], keep="first")
    rows_after = len(df_main)
    logger.info(
        f"Dedup: {rows_before} → {rows_after} rows "
        f"({rows_before - rows_after} duplicates removed)"
    )

    # Step 4b: Parse release_date — 3 mixed formats, coalesce results
    # Format 1: MM/DD/YYYY
    date_fmt1 = pd.to_datetime(df_main["release_date"], format="%m/%d/%Y", errors="coerce")
    # Format 2: YYYY-MM-DD
    date_fmt2 = pd.to_datetime(df_main["release_date"], format="%Y-%m-%d", errors="coerce")
    # Format 3: DD-MM-YYYY
    date_fmt3 = pd.to_datetime(df_main["release_date"], format="%d-%m-%Y", errors="coerce")
    # Coalesce — kuhanin ang unang successful parse
    parsed_date = date_fmt1.fillna(date_fmt2).fillna(date_fmt3)

    parsed_count = parsed_date.notna().sum()
    null_count = parsed_date.isna().sum()
    logger.info(f"Date parsing: {parsed_count} parsed, {null_count} still NULL")

    df_main["release_date"] = parsed_date

    # Step 4c: Cast budget, revenue, at id mula TEXT → NUMERIC bago mag-merge
    # 0 means unknown — palitan ng NaN para ma-fill ng enrichment
    # Cast IN-PLACE sa df_main para index-aligned ang merge result
    df_main["budget"] = pd.to_numeric(df_main["budget"], errors="coerce").replace(0, pd.NA)
    df_main["revenue"] = pd.to_numeric(df_main["revenue"], errors="coerce").replace(0, pd.NA)
    df_main["id"] = pd.to_numeric(df_main["id"], errors="coerce")

    # Step 4d: Merge with silver.movies_enriched (LEFT JOIN)
    df_merged = df_main.merge(
        df_enriched[["movie_id", "budget", "revenue"]],
        left_on="id",
        right_on="movie_id",
        how="left",
        suffixes=("_bronze", "_enriched"),
    )

    # Fill missing: bronze value muna, kung wala, enriched value
    # Gamit ang suffixed columns mula sa merge — index-aligned, hindi .values
    df_merged["budget"] = df_merged["budget_bronze"].fillna(df_merged["budget_enriched"])
    df_merged.loc[df_merged["budget"] == 0, "budget"] = pd.NA

    df_merged["revenue"] = df_merged["revenue_bronze"].fillna(df_merged["revenue_enriched"])
    df_merged.loc[df_merged["revenue"] == 0, "revenue"] = pd.NA

    # Step 4e: Trim title
    df_merged["title"] = df_merged["title"].str.strip()

    # Step 4f: Build final DataFrame
    df_movies = df_merged[["id", "title", "release_date", "budget", "revenue"]].copy()
    df_movies = df_movies.rename(columns={"id": "movie_id"})

    # Step 4g: Write sa silver.movies
    df_movies.to_sql(
        name="movies",
        schema="silver",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
    logger.info(f"Na-insert ang {len(df_movies)} rows sa silver.movies")

    assert len(df_movies) > 0, "Zero rows sa silver.movies — may problema sa transform"

    return df_movies


def transform_movie_genres(engine, df_extended, df_enriched):
    """
    Transform bronze.movie_extended genres → silver.movie_genres.
    Fill NULL genres mula sa enrichment, explode comma-separated → one row per genre.
    """
    logger.info("--- Simula ng silver.movie_genres transform ---")

    # Step 5a: Merge genres with enrichment para sa NULL filling
    df_extended["id"] = pd.to_numeric(df_extended["id"], errors="coerce")

    df_genres = df_extended[["id", "genres"]].copy()
    df_genres = df_genres.merge(
        df_enriched[["movie_id", "genres"]],
        left_on="id",
        right_on="movie_id",
        how="left",
        suffixes=("_bronze", "_enriched"),
    )

    # Fill NULL/empty bronze genres with enriched genres
    bronze_genres = df_genres["genres_bronze"].fillna("").str.strip()
    enriched_genres = df_genres["genres_enriched"].fillna("").str.strip()
    df_genres["genres_final"] = bronze_genres.where(bronze_genres != "", enriched_genres)

    # Step 5b: Filter out rows na walang genres kahit after enrichment
    df_genres = df_genres[df_genres["genres_final"] != ""].copy()

    # Step 5c: Explode — split comma-separated, one row per genre
    df_genres["genre"] = df_genres["genres_final"].str.split(",")
    df_exploded = df_genres[["id", "genre"]].explode("genre")
    df_exploded["genre"] = df_exploded["genre"].str.strip()

    # Drop empty strings at NULLs after explode
    df_exploded = df_exploded[
        df_exploded["genre"].notna() & (df_exploded["genre"] != "")
    ].copy()

    # Step 5d: Rename at finalize
    df_exploded = df_exploded.rename(columns={"id": "movie_id"})
    df_exploded = df_exploded[["movie_id", "genre"]]

    # Step 5e: Write sa silver.movie_genres
    df_exploded.to_sql(
        name="movie_genres",
        schema="silver",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
    logger.info(f"Na-insert ang {len(df_exploded)} rows sa silver.movie_genres")

    assert len(df_exploded) > 0, "Zero rows sa silver.movie_genres — may problema sa explode"

    return df_exploded


def transform_production_companies(engine, df_extended):
    """
    Transform bronze.movie_extended production_companies → silver.production_companies.
    Explode comma-separated → one row per company per movie.
    """
    logger.info("--- Simula ng silver.production_companies transform ---")

    # Step 6a: Parse id to integer
    df_companies = df_extended[["id", "production_companies"]].copy()
    df_companies["id"] = pd.to_numeric(df_companies["id"], errors="coerce")

    # Filter out rows na walang production companies
    df_companies = df_companies[
        df_companies["production_companies"].notna()
        & (df_companies["production_companies"].str.strip() != "")
    ].copy()

    # Step 6b: Explode — split comma-separated, one row per company
    df_companies["company_name"] = df_companies["production_companies"].str.split(",")
    df_exploded = df_companies[["id", "company_name"]].explode("company_name")
    df_exploded["company_name"] = df_exploded["company_name"].str.strip()

    # Drop empty strings at NULLs after explode
    df_exploded = df_exploded[
        df_exploded["company_name"].notna() & (df_exploded["company_name"] != "")
    ].copy()

    # Step 6c: Rename at finalize
    df_exploded = df_exploded.rename(columns={"id": "movie_id"})
    df_exploded = df_exploded[["movie_id", "company_name"]]

    # Step 6d: Write sa silver.production_companies
    df_exploded.to_sql(
        name="production_companies",
        schema="silver",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
    logger.info(f"Na-insert ang {len(df_exploded)} rows sa silver.production_companies")

    assert len(df_exploded) > 0, "Zero rows sa silver.production_companies — may problema sa explode"

    return df_exploded


def verify_counts(engine):
    """
    Final verification — i-count ang actual rows sa bawat output table.
    Assert lahat > 0 para siguradong may data.
    """
    logger.info("--- Final Verification ---")
    counts = {}

    for table_name in OUTPUT_TABLES:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM silver.{table_name}"))
            count = result.scalar()
        counts[table_name] = count
        logger.info(f"silver.{table_name}: {count} rows")

        assert count > 0, f"silver.{table_name} may 0 rows — may problema sa transform"

    return counts


def main():
    """
    Main function — i-transform ang Bronze data papunta sa Silver output tables.
    Steps: truncate, load, transform, write, verify.
    """
    logger.info("=== Simula ng Silver Transform ===")

    try:
        engine = get_engine()
        logger.info("Database connection established")

        # Step 1: TRUNCATE lahat ng output tables — idempotent
        # Tables na-create na ng silver_ddl.py — hindi na kailangan i-create dito
        truncate_output_tables(engine)

        # Step 2: Load source data
        df_main, df_extended, df_enriched = load_source_data(engine)

        # Step 3: Transform at write silver.movies
        transform_movies(engine, df_main, df_enriched)

        # Step 4: Transform at write silver.movie_genres
        transform_movie_genres(engine, df_extended, df_enriched)

        # Step 5: Transform at write silver.production_companies
        transform_production_companies(engine, df_extended)

        # Step 6: Final verification
        verify_counts(engine)

        engine.dispose()
        logger.info("=== Silver Transform TAPOS NA ===")

    except Exception as e:
        logger.error(f"Error sa Silver Transform: {e}")
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Non-zero exit code para ma-detect ng Airflow BashOperator ang failure
        logger.error(f"Unhandled exception: {e}")
        sys.exit(1)
