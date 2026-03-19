"""
Bronze DDL Script
Gumagawa ng bronze schema at raw tables sa Postgres.
Lahat ng columns ay TEXT — walang type casting sa Bronze layer.
Bawat column may COMMENT para madaling maintindihan ng team.
"""

import os
import sys
from loguru import logger
from sqlalchemy import create_engine, text

# === Loguru Configuration ===
# Dalawang sinks: stdout para sa real-time monitoring, file para sa audit trail
logger.remove()
logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
logger.add("/logs/bronze/bronze.log", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", rotation="10 MB")

# === Table Definitions ===
# Mga column names na galing EXACTLY sa CSV headers
# Lahat TEXT kasi Bronze layer = raw data, walang transformation
TABLES = {
    "movies_main": {
        "columns": {
            "id": "Unique identifier ng movie mula sa TMDB",
            "title": "Opisyal na title ng movie",
            "release_date": "Petsa ng release ng movie (raw string format)",
            "budget": "Production budget ng movie sa USD (raw string, hindi pa na-cast)",
            "revenue": "Box office revenue ng movie sa USD (raw string, hindi pa na-cast)",
        }
    },
    "movie_extended": {
        "columns": {
            "id": "Unique identifier ng movie mula sa TMDB (foreign key sa movies_main)",
            "genres": "Comma-separated list ng genres ng movie",
            "production_companies": "Mga production company na gumawa ng movie",
            "production_countries": "JSON-like string ng mga bansang pinag-produce-an ng movie",
            "spoken_languages": "JSON-like string ng mga language na ginamit sa movie",
        }
    },
}


def get_engine():
    """Gawa ng SQLAlchemy engine gamit ang environment variables."""
    db_url = (
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    return create_engine(db_url)


def create_schema(engine):
    """Gawa ng bronze schema kung wala pa."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.commit()
    logger.info("Schema 'bronze' na-create na (o existing na)")


def create_table(engine, table_name, columns):
    """
    Gawa ng table sa bronze schema kung wala pa.
    Lahat ng columns ay TEXT — raw data, walang type casting.
    """
    # I-build ang CREATE TABLE statement na may lahat ng columns as TEXT
    col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
    create_sql = f'CREATE TABLE IF NOT EXISTS bronze.{table_name} ({col_defs})'

    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()
    logger.info(f"Table 'bronze.{table_name}' na-create na (o existing na)")


def add_column_comments(engine, table_name, columns):
    """
    Mag-add ng COMMENT sa bawat column para madaling maintindihan
    ng ibang developers kung ano ang laman ng column.
    """
    with engine.connect() as conn:
        for col_name, description in columns.items():
            comment_sql = text(
                f"COMMENT ON COLUMN bronze.{table_name}.\"{col_name}\" IS :desc"
            )
            conn.execute(comment_sql, {"desc": description})
            logger.info(f"Comment added sa bronze.{table_name}.\"{col_name}\": {description}")
        conn.commit()


def main():
    """Main function — i-run ang buong DDL process."""
    logger.info("=== Simula ng Bronze DDL ===")

    try:
        engine = get_engine()
        logger.info("Database connection established")

        # Step 1: Gawa ng schema
        create_schema(engine)

        # Step 2: Gawa ng bawat table at i-add ang comments
        for table_name, config in TABLES.items():
            create_table(engine, table_name, config["columns"])
            add_column_comments(engine, table_name, config["columns"])

        logger.info("=== Bronze DDL TAPOS NA — lahat ng tables at comments na-create na ===")

    except Exception as e:
        logger.error(f"Error sa Bronze DDL: {e}")
        raise


if __name__ == "__main__":
    main()
