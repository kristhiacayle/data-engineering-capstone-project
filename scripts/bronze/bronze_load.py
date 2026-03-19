"""
Bronze Load Script
Nagba-basa ng CSV files mula sa GCS bucket at nilo-load sa bronze schema.
Lahat ng data naka-read as string (dtype=str) — walang type inference sa Bronze.
TRUNCATE muna bago INSERT para idempotent (safe i-rerun).
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
logger.add("/logs/bronze/bronze.log", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", rotation="10 MB")

# === Constants ===
# GCS bucket name at mga filename na ilo-load
GCS_BUCKET = "internship-capstone-movies"
FILES_TO_LOAD = {
    "movies_main.csv": "movies_main",
    "movie_extended.csv": "movie_extended",
}


def get_engine():
    """Gawa ng SQLAlchemy engine gamit ang environment variables."""
    db_url = (
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    return create_engine(db_url)


def load_file(engine, filename, table_name):
    """
    I-load ang isang CSV file mula sa GCS papunta sa bronze table.

    Steps:
    1. Basahin ang CSV mula GCS (lahat ng columns as string)
    2. TRUNCATE ang existing na data (para walang duplicates pag ni-rerun)
    3. INSERT gamit ang df.to_sql() na may batch insert
    4. Verify ang row count sa database vs DataFrame
    """
    gcs_path = f"gs://{GCS_BUCKET}/{filename}"
    logger.info(f"Simula ng load para sa {filename}")

    try:
        # Step 1: Basahin ang CSV mula sa GCS
        # dtype=str para lahat ng values ay raw string — Bronze layer, walang type casting
        df = pd.read_csv(gcs_path, dtype=str)
        logger.info(f"Na-read ang {filename}: {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"Columns: {list(df.columns)}")

        # Step 2: TRUNCATE para idempotent — safe i-rerun nang walang duplicates
        # Nag-preserve ng DDL at COMMENTs, data lang ang matatanggal
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE bronze.{table_name}"))
            conn.commit()
        logger.info(f"Na-truncate ang bronze.{table_name}")

        # Step 3: INSERT gamit ang df.to_sql()
        # method='multi' para batch insert (mas mabilis)
        # chunksize=5000 para hindi mag-OOM sa malaking datasets
        df.to_sql(
            name=table_name,
            schema="bronze",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )
        logger.info(f"Na-insert ang {len(df)} rows sa bronze.{table_name}")

        # Step 4: Verify — i-count ang actual rows sa database
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM bronze.{table_name}"))
            db_count = result.scalar()

        logger.info(f"Verification: DataFrame={len(df)} rows, Database={db_count} rows")

        if db_count != len(df):
            logger.warning(
                f"Row count mismatch sa {table_name}! "
                f"Expected {len(df)}, got {db_count}"
            )
        else:
            logger.info(f"Row count MATCH — {table_name} load successful")

        return db_count

    except Exception as e:
        logger.error(f"Error sa pag-load ng {filename}: {e}")
        raise


def main():
    """Main function — i-load lahat ng CSV files sa bronze schema."""
    logger.info("=== Simula ng Bronze Load ===")

    try:
        engine = get_engine()
        logger.info("Database connection established")

        # I-load ang bawat file isa-isa
        for filename, table_name in FILES_TO_LOAD.items():
            load_file(engine, filename, table_name)

        logger.info("=== Bronze Load TAPOS NA — lahat ng files na-load na ===")

    except Exception as e:
        logger.error(f"Error sa Bronze Load: {e}")
        raise


if __name__ == "__main__":
    main()
