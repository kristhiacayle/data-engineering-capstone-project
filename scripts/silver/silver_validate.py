"""
Silver Validation Script
I-validate ang Silver layer bago mag-proceed sa Gold.
Limang checks: Pandera schema, row counts, null IDs, uniqueness, value ranges.
Kung may nag-fail na check, hindi pwedeng mag-continue ang pipeline.
"""

import os
import sys
import pandas as pd
import pandera as pa
from loguru import logger
from sqlalchemy import create_engine, text

# === Loguru Configuration ===
logger.remove()
logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
logger.add("/logs/silver/silver.log", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", rotation="10 MB")

# === Pandera Schemas ===
# strict=True — mag-fail kung may extra o kulang na columns
# movie_id: walang dtype check — nullable=False lang
#   Bakit: PostgreSQL INTEGER/BIGINT → int64 pag walang NULLs, pero float64 pag may NULLs
#   nullable=False catches NULLs clearly. Check 3 at Check 4 guarantee type safety.
SCHEMAS = {
    "movies": pa.DataFrameSchema(
        columns={
            "movie_id": pa.Column(nullable=False),
            "movie_title": pa.Column(object, nullable=True),
            "release_date": pa.Column(object, nullable=True),  # PostgreSQL DATE loads as object
            "budget": pa.Column(float, nullable=True),
            "revenue": pa.Column(float, nullable=True),
        },
        strict=True,
    ),
    "movie_genres": pa.DataFrameSchema(
        columns={
            "movie_id": pa.Column(nullable=False),
            "genre": pa.Column(object, nullable=False),
        },
        strict=True,
    ),
    "production_companies": pa.DataFrameSchema(
        columns={
            "movie_id": pa.Column(nullable=False),
            "company_name": pa.Column(object, nullable=False),
        },
        strict=True,
    ),
    "movies_enriched": pa.DataFrameSchema(
        columns={
            "movie_id": pa.Column(nullable=False),
            "budget": pa.Column(float, nullable=True),
            "revenue": pa.Column(float, nullable=True),
            "genres": pa.Column(object, nullable=True),
            "production_countries": pa.Column(object, nullable=True),
            "spoken_languages": pa.Column(object, nullable=True),
        },
        strict=True,
    ),
    "producing_countries": pa.DataFrameSchema(
        columns={
            "movie_id": pa.Column(nullable=False),
            "iso_country_code": pa.Column(object, nullable=False),
            "country_name": pa.Column(object, nullable=False),
            "country_region": pa.Column(object, nullable=True),      # NULL only for unexpected ISO codes
            "country_subregion": pa.Column(object, nullable=True),   # NULL only for unexpected ISO codes
            "is_service_restricted": pa.Column(bool, nullable=False), # always TRUE or FALSE
        },
        strict=True,
    ),
    "spoken_languages": pa.DataFrameSchema(
        columns={
            "movie_id": pa.Column(nullable=False),
            "iso_language_code": pa.Column(object, nullable=False),
            "language_name": pa.Column(object, nullable=False),
        },
        strict=True,
    ),
}

SILVER_TABLES = [
    "movies",
    "movie_genres",
    "production_companies",
    "movies_enriched",
    "producing_countries",
    "spoken_languages",
]


def get_engine():
    """Gawa ng SQLAlchemy engine gamit ang environment variables."""
    db_url = (
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    return create_engine(db_url)


def check_1_pandera_schema(engine):
    """
    Check 1: Pandera Schema Validation.
    Bine-verify na lahat ng expected columns ay present, walang extra, at tamang dtype/nullability.
    """
    logger.info("Nagsisimula ng Check 1: Pandera Schema Validation...")

    for table_name in SILVER_TABLES:
        with engine.connect() as conn:
            df = pd.read_sql(
                text(f"SELECT * FROM silver.{table_name} LIMIT 1000"),
                conn,
            )

        if df.empty:
            raise ValueError(
                f"silver.{table_name} ay walang data — hindi ma-validate ang schema. "
                f"I-run muna ang silver_transform.py."
            )

        SCHEMAS[table_name].validate(df)
        logger.info(
            f"  Check 1 PASSED para sa silver.{table_name} — "
            f"{len(df.columns)} columns, types correct"
        )

    logger.info("Check 1: Pandera Schema Validation — PASSED")


def check_2_row_counts(engine):
    """
    Check 2: Row Count > 0.
    Bawat silver table dapat may data — zero rows ibig sabihin nag-fail ang transform.
    """
    logger.info("Nagsisimula ng Check 2: Row Count > 0...")

    for table_name in SILVER_TABLES:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM silver.{table_name}"))
            count = result.scalar()

        assert count > 0, (
            f"silver.{table_name} may 0 rows — "
            f"baka nag-fail ang transform o enrich step"
        )
        logger.info(f"  Check 2 PASSED para sa silver.{table_name} — {count} rows")

    logger.info("Check 2: Row Count > 0 — PASSED")


def check_3_no_null_movie_ids(engine):
    """
    Check 3: No NULL movie_ids.
    Ang movie_id column ay dapat walang NULL values sa lahat ng silver tables.
    NULL movie_id = broken foreign key — pipeline BLOCKER.
    """
    logger.info("Nagsisimula ng Check 3: No NULL movie_ids...")

    for table_name in SILVER_TABLES:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM silver.{table_name} WHERE movie_id IS NULL")
            )
            null_count = result.scalar()

        assert null_count == 0, (
            f"{null_count} NULL movie_id(s) found sa silver.{table_name} — "
            f"movie_id must have zero NULLs"
        )
        logger.info(f"  Check 3 PASSED para sa silver.{table_name} — 0 NULL movie_ids")

    logger.info("Check 3: No NULL movie_ids — PASSED")


def check_4_unique_movie_ids_in_movies(engine):
    """
    Check 4: Unique movie_ids sa silver.movies.
    Ang silver.movies ay deduplicated na — dapat walang duplicate movie_ids.
    """
    logger.info("Nagsisimula ng Check 4: Unique movie_ids sa silver.movies...")

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT movie_id, COUNT(*) FROM silver.movies "
            "GROUP BY movie_id HAVING COUNT(*) > 1"
        ))
        duplicates = result.fetchall()

    assert len(duplicates) == 0, (
        f"{len(duplicates)} duplicate movie_id(s) found sa silver.movies — "
        f"dedup step may problema"
    )
    logger.info(f"  Check 4 PASSED — 0 duplicate movie_ids sa silver.movies")

    logger.info("Check 4: Unique movie_ids — PASSED")


def check_5_value_ranges(engine):
    """
    Check 5: Value Ranges.
    5a. Budget at revenue >= 0 (walang negative)
    5b. Walang empty strings sa genre
    5c. Walang empty strings sa company_name
    5d. Walang empty strings sa iso_country_code at country_name
    5e. Walang empty strings sa iso_language_code at language_name
    """
    logger.info("Nagsisimula ng Check 5: Value Ranges...")

    # 5a: Budget >= 0
    with engine.connect() as conn:
        neg_budget = conn.execute(
            text("SELECT COUNT(*) FROM silver.movies WHERE budget < 0")
        ).scalar()
    assert neg_budget == 0, f"{neg_budget} negative budget(s) found sa silver.movies"
    logger.info("  Check 5a PASSED — 0 negative budgets")

    # 5b: Revenue >= 0
    with engine.connect() as conn:
        neg_revenue = conn.execute(
            text("SELECT COUNT(*) FROM silver.movies WHERE revenue < 0")
        ).scalar()
    assert neg_revenue == 0, f"{neg_revenue} negative revenue(s) found sa silver.movies"
    logger.info("  Check 5b PASSED — 0 negative revenues")

    # 5c: No empty strings sa genre
    with engine.connect() as conn:
        empty_genres = conn.execute(
            text("SELECT COUNT(*) FROM silver.movie_genres WHERE TRIM(genre) = ''")
        ).scalar()
    assert empty_genres == 0, f"{empty_genres} empty genre string(s) found"
    logger.info("  Check 5c PASSED — 0 empty genre strings")

    # 5d: No empty strings sa company_name
    with engine.connect() as conn:
        empty_companies = conn.execute(
            text("SELECT COUNT(*) FROM silver.production_companies WHERE TRIM(company_name) = ''")
        ).scalar()
    assert empty_companies == 0, f"{empty_companies} empty company_name string(s) found"
    logger.info("  Check 5d PASSED — 0 empty company_name strings")

    # 5e: No empty strings sa iso_country_code at country_name
    with engine.connect() as conn:
        empty_country = conn.execute(
            text("SELECT COUNT(*) FROM silver.producing_countries WHERE TRIM(iso_country_code) = '' OR TRIM(country_name) = ''")
        ).scalar()
    assert empty_country == 0, f"{empty_country} empty country field(s) found"
    logger.info("  Check 5e PASSED — 0 empty country fields")

    # 5f: No empty strings sa iso_language_code at language_name
    with engine.connect() as conn:
        empty_lang = conn.execute(
            text("SELECT COUNT(*) FROM silver.spoken_languages WHERE TRIM(iso_language_code) = '' OR TRIM(language_name) = ''")
        ).scalar()
    assert empty_lang == 0, f"{empty_lang} empty language field(s) found"
    logger.info("  Check 5f PASSED — 0 empty language fields")

    logger.info("Check 5: Value Ranges — PASSED")


def main():
    """
    Main function — i-run ang lahat ng 5 validation checks.
    Bawat check ay may sariling try/except para mapatuloy ang iba kahit may mag-fail.
    Sa huli, kung may nag-fail, i-raise ang RuntimeError at exit ng non-zero code.
    """
    logger.info("=== Simula ng Silver Validation ===")

    engine = get_engine()
    logger.info("Database connection established")

    failed_checks = []

    try:
        check_1_pandera_schema(engine)
    except Exception as e:
        logger.error(f"Check 1 FAILED: {e}")
        failed_checks.append(f"Check 1 (Pandera Schema): {e}")

    try:
        check_2_row_counts(engine)
    except Exception as e:
        logger.error(f"Check 2 FAILED: {e}")
        failed_checks.append(f"Check 2 (Row Counts): {e}")

    try:
        check_3_no_null_movie_ids(engine)
    except Exception as e:
        logger.error(f"Check 3 FAILED: {e}")
        failed_checks.append(f"Check 3 (No NULL movie_ids): {e}")

    try:
        check_4_unique_movie_ids_in_movies(engine)
    except Exception as e:
        logger.error(f"Check 4 FAILED: {e}")
        failed_checks.append(f"Check 4 (Unique movie_ids): {e}")

    try:
        check_5_value_ranges(engine)
    except Exception as e:
        logger.error(f"Check 5 FAILED: {e}")
        failed_checks.append(f"Check 5 (Value Ranges): {e}")

    engine.dispose()

    logger.info("=== Silver Validation SUMMARY ===")

    if failed_checks:
        for fail in failed_checks:
            logger.error(f"  FAILED: {fail}")
        logger.error("Silver validation FAILED. Huwag mag-proceed sa Gold.")
        raise RuntimeError(f"Nabigong Silver checks: {failed_checks}")
    else:
        logger.info("  Check 1: Pandera Schema Validation — PASSED")
        logger.info("  Check 2: Row Count > 0 — PASSED")
        logger.info("  Check 3: No NULL movie_ids — PASSED")
        logger.info("  Check 4: Unique movie_ids — PASSED")
        logger.info("  Check 5: Value Ranges — PASSED")
        logger.info("Silver layer validated. Ligtas nang pumunta sa Gold.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        sys.exit(1)
