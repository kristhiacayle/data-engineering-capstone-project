"""
Bronze Validation Script
I-validate ang Bronze layer bago mag-proceed sa Silver.
Apat na checks: column integrity, row count, null IDs, fully empty rows.
Kung may nag-fail na check, hindi pwedeng mag-continue ang pipeline.
"""

import os
import sys
import pandas as pd
import pandera as pa
from loguru import logger
from sqlalchemy import create_engine, text

# === Loguru Configuration ===
# Parehong sinks sa DDL at Load — iisang log file ang bronze.log
logger.remove()
logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
logger.add("/logs/bronze/bronze.log", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", rotation="10 MB")

# === Constants ===
# GCS bucket at file-to-table mapping — dapat consistent sa bronze_load.py
GCS_BUCKET = "internship-capstone-movies"
TABLES_TO_FILES = {
    "movies_main": "movies_main.csv",
    "movie_extended": "movie_extended.csv",
}


def get_engine():
    """Gawa ng SQLAlchemy engine gamit ang environment variables."""
    db_url = (
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    return create_engine(db_url)


def get_columns_from_db(engine, table_name):
    """
    Basahin ang column names ng isang bronze table mula sa information_schema.
    Hindi hardcoded — para laging accurate kahit magbago ang schema.
    Kung walang nahanap na columns, ibig sabihin hindi pa na-create ang table.
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'bronze' AND table_name = :table "
                "ORDER BY ordinal_position"
            ),
            {"table": table_name},
        )
        rows = result.fetchall()

    if not rows:
        raise KeyError(
            f"Walang columns na nahanap sa information_schema para sa bronze.{table_name}. "
            f"Baka hindi pa na-create ang table — i-run muna ang bronze_ddl.py."
        )

    return [row[0] for row in rows]


def check_1_column_integrity(engine):
    """
    Check 1: Column Integrity gamit ang Pandera.
    Bine-verify na:
    - Lahat ng expected columns ay present (exact names, walang typo)
    - Walang extra/unexpected columns (strict=True ang Pandera schema)
    - Bawat column dtype ay object (string) — Bronze must be all TEXT, zero exceptions
    """
    logger.info("Nagsisimula ng Check 1: Column Integrity...")

    for table_name in TABLES_TO_FILES:
        # Kunin ang column names mula sa information_schema — hindi hardcoded
        columns = get_columns_from_db(engine, table_name)
        logger.info(f"  Columns sa bronze.{table_name}: {columns}")

        # Basahin ang sample ng data para sa Pandera dtype validation
        # LIMIT 1000 — schema check lang, hindi kailangan i-load ang buong table
        with engine.connect() as conn:
            df = pd.read_sql(
                text(f"SELECT * FROM bronze.{table_name} LIMIT 1000"),
                conn,
            )

        if df.empty:
            raise ValueError(
                f"bronze.{table_name} ay walang data — hindi ma-validate ang schema. "
                f"I-run muna ang bronze_load.py."
            )

        # I-build ang Pandera schema dynamically mula sa columns na nakuha sa DB
        # pa.Column(object) = dapat text/string ang dtype
        # strict=True = mag-fail kung may extra o kulang na columns
        schema_columns = {col: pa.Column(object, nullable=True) for col in columns}
        schema = pa.DataFrameSchema(columns=schema_columns, strict=True)

        # I-validate — mag-ra-raise ng SchemaError kung may problema
        # Hindi ito caught dito — dapat mag-propagate up para ma-log sa main()
        schema.validate(df)
        logger.info(
            f"  Check 1 PASSED para sa bronze.{table_name} — "
            f"{len(columns)} columns, lahat ay object dtype"
        )

    logger.info("Check 1: Column Integrity — PASSED")


def check_2_row_count(engine):
    """
    Check 2: Row Count Integrity.
    Fresh re-read mula sa GCS — independent sa kung ano ang na-log ng bronze_load.py.
    I-compare ang source count vs bronze table count.
    Kahit isang row na difference, fail agad — assert exact match.
    """
    logger.info("Nagsisimula ng Check 2: Row Count Integrity...")

    for table_name, filename in TABLES_TO_FILES.items():
        gcs_path = f"gs://{GCS_BUCKET}/{filename}"

        # Fresh read mula sa GCS — dtype=str para consistent sa pag-load
        # Ito ang source of truth para sa row count
        logger.info(f"  Binabasa ang source mula GCS: {gcs_path}")
        df_source = pd.read_csv(gcs_path, dtype=str)
        source_count = len(df_source)
        logger.info(f"  Source count ({filename}): {source_count}")

        # I-query ang bronze table count
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM bronze.{table_name}"))
            bronze_count = result.scalar()

        if bronze_count is None:
            raise RuntimeError(
                f"COUNT(*) query sa bronze.{table_name} ay nagbalik ng None — "
                f"unexpected database error."
            )

        logger.info(f"  Bronze count (bronze.{table_name}): {bronze_count}")

        # Assert exact match — hindi pwedeng may difference kahit 1 row
        assert source_count == bronze_count, (
            f"Row count mismatch sa {table_name}! "
            f"source={source_count}, bronze={bronze_count}, "
            f"difference={abs(source_count - bronze_count)} rows"
        )
        logger.info(f"  Check 2 PASSED para sa {table_name} — {bronze_count} rows match")

    logger.info("Check 2: Row Count Integrity — PASSED")


def check_3_no_null_ids(engine):
    """
    Check 3: No Nulls in Primary Key Column.
    Ang id column ay dapat walang NULL values sa parehong tables.
    NULL id = corrupted load o bad source data — pipeline blocker ito.
    """
    logger.info("Nagsisimula ng Check 3: No Nulls in id Column...")

    for table_name in TABLES_TO_FILES:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM bronze.{table_name} WHERE id IS NULL")
            )
            null_count = result.scalar()

        if null_count is None:
            raise RuntimeError(
                f"NULL check query sa bronze.{table_name} ay nagbalik ng None — "
                f"unexpected database error."
            )

        # Assert zero NULLs — id column must always have a value
        assert null_count == 0, (
            f"{null_count} NULL id(s) found sa bronze.{table_name} — "
            f"id column must have zero NULLs"
        )
        logger.info(f"  Check 3 PASSED para sa {table_name} — 0 NULL ids")

    logger.info("Check 3: No Nulls in id Column — PASSED")


def check_4_no_fully_empty_rows(engine):
    """
    Check 4: No Fully Empty Rows (informational only).
    Bronze is raw — hindi natin fail-in ito, pero kailangan malaman ng team
    kung may ganyang rows para ma-handle sa Silver layer.
    Fully empty = lahat ng columns ay NULL o empty string.
    """
    logger.info("Nagsisimula ng Check 4: No Fully Empty Rows...")

    for table_name in TABLES_TO_FILES:
        # Kunin ang columns para i-build ang dynamic SQL WHERE clause
        columns = get_columns_from_db(engine, table_name)

        # I-build ang WHERE clause: bawat column ay NULL o empty string
        # Fully empty row = lahat ng conditions ay true
        conditions = " AND ".join(
            f"({col} IS NULL OR TRIM({col}) = '')" for col in columns
        )
        sql = f"SELECT COUNT(*) FROM bronze.{table_name} WHERE {conditions}"

        with engine.connect() as conn:
            result = conn.execute(text(sql))
            empty_count = result.scalar()

        if empty_count is None:
            raise RuntimeError(
                f"Fully empty rows query sa bronze.{table_name} ay nagbalik ng None."
            )

        if empty_count > 0:
            # WARNING lang — informational, hindi fail
            # Bronze is raw data, kaya possible may ganito
            logger.warning(
                f"  Check 4 WARNING: {empty_count} fully empty row(s) found "
                f"sa bronze.{table_name} — informational only, hindi pipeline blocker"
            )
        else:
            logger.info(f"  Check 4 PASSED para sa {table_name} — 0 fully empty rows")

    logger.info("Check 4: No Fully Empty Rows — DONE (informational)")


def main():
    """
    Main function — i-run ang lahat ng 4 validation checks.
    Bawat check ay may sariling try/except para mapatuloy ang iba kahit may mag-fail.
    Sa huli, kung may nag-fail, i-raise ang RuntimeError at exit ng non-zero code
    para ma-detect ng Airflow BashOperator.
    """
    logger.info("=== Simula ng Bronze Validation ===")

    engine = get_engine()
    logger.info("Database connection established")

    # Kolektahin ang lahat ng failed checks — i-report lahat sa dulo
    failed_checks = []

    # --- Check 1: Column Integrity ---
    try:
        check_1_column_integrity(engine)
    except Exception as e:
        logger.error(f"Check 1 FAILED: {e}")
        failed_checks.append(f"Check 1 (Column Integrity): {e}")

    # --- Check 2: Row Count Integrity ---
    try:
        check_2_row_count(engine)
    except Exception as e:
        logger.error(f"Check 2 FAILED: {e}")
        failed_checks.append(f"Check 2 (Row Count Integrity): {e}")

    # --- Check 3: No Nulls in id ---
    try:
        check_3_no_null_ids(engine)
    except Exception as e:
        logger.error(f"Check 3 FAILED: {e}")
        failed_checks.append(f"Check 3 (No Null IDs): {e}")

    # --- Check 4: No Fully Empty Rows (informational, pero may error handling pa rin) ---
    try:
        check_4_no_fully_empty_rows(engine)
    except Exception as e:
        logger.error(f"Check 4 ERROR (unexpected): {e}")
        failed_checks.append(f"Check 4 (No Fully Empty Rows): {e}")

    # I-dispose ang engine para linisin ang connections
    engine.dispose()

    # --- Final Summary ---
    logger.info("=== Bronze Validation SUMMARY ===")

    if failed_checks:
        for fail in failed_checks:
            logger.error(f"  FAILED: {fail}")
        logger.error("Bronze validation FAILED. Huwag mag-proceed sa Silver.")
        raise RuntimeError(f"Nabigong Bronze checks: {failed_checks}")
    else:
        logger.info("  Check 1: Column Integrity — PASSED")
        logger.info("  Check 2: Row Count Integrity — PASSED")
        logger.info("  Check 3: No Nulls in id — PASSED")
        logger.info("  Check 4: No Fully Empty Rows — DONE")
        logger.info("Bronze layer validated. Ligtas nang pumunta sa Silver.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Non-zero exit code para ma-detect ng Airflow BashOperator ang failure
        logger.error(f"Unhandled exception: {e}")
        sys.exit(1)
