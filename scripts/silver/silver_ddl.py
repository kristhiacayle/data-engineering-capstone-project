"""
Silver DDL Script
Gumagawa ng silver schema at typed tables sa Postgres.
Hindi na lahat TEXT — may INTEGER, DATE, NUMERIC na ang columns.
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
logger.add("/logs/silver/silver.log", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", rotation="10 MB")

# === Table Definitions ===
# Typed columns na — hindi na lahat TEXT tulad ng Bronze
# Bawat table may column name, SQL type, at description para sa COMMENT
TABLES = {
    "movies_main": {
        "columns": {
            "id": {"type": "INTEGER", "comment": "Unique identifier ng movie mula sa TMDB"},
            "title": {"type": "TEXT", "comment": "Opisyal na title ng movie"},
            "release_date": {"type": "DATE", "comment": "Petsa ng release ng movie (na-cast na sa DATE)"},
            "budget": {"type": "NUMERIC", "comment": "Production budget ng movie sa USD (na-cast na sa NUMERIC, 0 = unknown)"},
            "revenue": {"type": "NUMERIC", "comment": "Box office revenue ng movie sa USD (na-cast na sa NUMERIC, 0 = unknown)"},
        }
    },
    "movie_extended": {
        "columns": {
            "id": {"type": "INTEGER", "comment": "Unique identifier ng movie mula sa TMDB (foreign key sa movies_main)"},
            "genres": {"type": "TEXT", "comment": "Comma-separated list ng genres ng movie"},
            "production_companies": {"type": "TEXT", "comment": "Mga production company na gumawa ng movie"},
            "production_countries": {"type": "TEXT", "comment": "JSON-like string ng mga bansang pinag-produce-an ng movie"},
            "spoken_languages": {"type": "TEXT", "comment": "JSON-like string ng mga language na ginamit sa movie"},
        }
    },
    "movies_enriched": {
        "columns": {
            "movie_id": {"type": "INTEGER", "comment": "TMDB movie ID, ginagamit para i-join sa silver.movies_main"},
            "budget": {"type": "NUMERIC", "comment": "Budget mula sa TMDB API (puno ng missing values mula bronze)"},
            "revenue": {"type": "NUMERIC", "comment": "Revenue mula sa TMDB API (puno ng missing values mula bronze)"},
            "genres": {"type": "TEXT", "comment": "Genres mula sa TMDB API (puno ng NULL genres mula bronze)"},
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
    """Gawa ng silver schema kung wala pa."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        conn.commit()
    logger.info("Schema 'silver' na-create na (o existing na)")


def create_table(engine, table_name, columns):
    """
    Gawa ng table sa silver schema kung wala pa.
    Typed columns na — INTEGER, DATE, NUMERIC, TEXT depende sa definition.
    """
    # I-build ang CREATE TABLE statement na may typed columns
    col_defs = ", ".join(
        f'"{col}" {config["type"]}' for col, config in columns.items()
    )
    create_sql = f"CREATE TABLE IF NOT EXISTS silver.{table_name} ({col_defs})"

    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()
    logger.info(f"Table 'silver.{table_name}' na-create na (o existing na)")


def add_column_comments(engine, table_name, columns):
    """
    Mag-add ng COMMENT sa bawat column para madaling maintindihan
    ng ibang developers kung ano ang laman ng column.
    """
    with engine.connect() as conn:
        for col_name, config in columns.items():
            comment_sql = text(
                f'COMMENT ON COLUMN silver.{table_name}."{col_name}" IS :desc'
            )
            conn.execute(comment_sql, {"desc": config["comment"]})
            logger.info(f'Comment added sa silver.{table_name}."{col_name}": {config["comment"]}')
        conn.commit()


def main():
    """Main function — i-run ang buong Silver DDL process."""
    logger.info("=== Simula ng Silver DDL ===")

    try:
        engine = get_engine()
        logger.info("Database connection established")

        # Step 1: Gawa ng schema
        create_schema(engine)

        # Step 2: Gawa ng bawat table at i-add ang comments
        for table_name, config in TABLES.items():
            create_table(engine, table_name, config["columns"])
            add_column_comments(engine, table_name, config["columns"])

        logger.info("=== Silver DDL TAPOS NA — lahat ng tables at comments na-create na ===")

    except Exception as e:
        logger.error(f"Error sa Silver DDL: {e}")
        raise


if __name__ == "__main__":
    main()
