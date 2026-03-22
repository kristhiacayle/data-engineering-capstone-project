"""
Silver Enrichment Script
Nagba-basa ng movies mula sa Bronze na may missing budget/revenue/genres/countries/languages,
tinatawagan ang TMDB API para kunin ang tamang values,
at sine-save ang results sa silver.movies_enriched.

TMDB API: 60 concurrent threads, tatlong keys sa round-robin — handles 429 with backoff.
Para sa ~38K candidates, expect ~3-5 minutes runtime.
"""

import os
import sys
import time
import threading
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
from sqlalchemy import create_engine, text

# === Loguru Configuration ===
# Dalawang sinks: stdout para sa real-time monitoring, file para sa audit trail
logger.remove()
logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
logger.add("/logs/silver/silver.log", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", rotation="10 MB")

# === Constants ===
TMDB_BASE_URL = "https://api.themoviedb.org/3/movie"
REQUEST_TIMEOUT = 10   # 10 seconds timeout per request
MAX_WORKERS = 60       # 60 concurrent threads — 3 APIs × 20 workers each
PROGRESS_INTERVAL = 500

# Thread-local storage — bawat worker thread ay may sariling requests.Session
_thread_local = threading.local()

# API key rotation counter — round-robin across all keys
_api_key_counter = 0
_api_key_lock = threading.Lock()


def get_engine():
    """Gawa ng SQLAlchemy engine gamit ang environment variables."""
    db_url = (
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    return create_engine(db_url)


def get_session():
    """Kuhanin ang thread-local requests.Session — gawa kung wala pa."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    return _thread_local.session


def get_next_api_key(api_keys):
    """
    Kuhanin ang next API key in round-robin fashion.
    Thread-safe gamit ang lock.
    """
    global _api_key_counter
    with _api_key_lock:
        key = api_keys[_api_key_counter % len(api_keys)]
        _api_key_counter += 1
    return key


def get_candidates(engine):
    """
    Kunin ang lahat ng movie IDs na kailangan i-enrich mula sa Bronze.
    Tatlong sources:
    1. movies_main — kung budget=0/NULL o revenue=0/NULL
    2. movie_extended — kung genres ay NULL o empty
    3. movie_extended — kung production_countries o spoken_languages ay NULL o empty array
    Pinagsasama (union) para isang API call lang per movie.
    """
    with engine.connect() as conn:
        result_financial = conn.execute(text(
            "SELECT id FROM bronze.movies_main "
            "WHERE budget IS NULL OR TRIM(budget) = '' OR TRIM(budget) = '0' "
            "   OR revenue IS NULL OR TRIM(revenue) = '' OR TRIM(revenue) = '0'"
        ))
        financial_ids = {row[0] for row in result_financial.fetchall()}
    logger.info(f"Movies na may missing budget/revenue: {len(financial_ids)}")

    with engine.connect() as conn:
        result_genres = conn.execute(text(
            "SELECT id FROM bronze.movie_extended "
            "WHERE genres IS NULL OR TRIM(genres) = ''"
        ))
        genre_ids = {row[0] for row in result_genres.fetchall()}
    logger.info(f"Movies na may missing genres: {len(genre_ids)}")

    with engine.connect() as conn:
        result_geo = conn.execute(text(
            "SELECT id FROM bronze.movie_extended "
            "WHERE production_countries IS NULL OR TRIM(production_countries) = '' OR production_countries = '[]' "
            "   OR spoken_languages IS NULL OR TRIM(spoken_languages) = '' OR spoken_languages = '[]'"
        ))
        geo_ids = {row[0] for row in result_geo.fetchall()}
    logger.info(f"Movies na may missing production_countries o spoken_languages: {len(geo_ids)}")

    all_ids = financial_ids | genre_ids | geo_ids
    logger.info(f"Total unique candidates para sa TMDB enrichment: {len(all_ids)}")

    return all_ids


def call_tmdb_api(movie_id, api_key):
    """
    Tawagan ang TMDB API para sa isang movie gamit ang thread-local session.
    Returns: dict na may budget, revenue, genres, production_countries, spoken_languages
             kung successful, None kung failed.

    Error handling:
    - 404: movie not found — log warning, return None
    - 429: rate limited — wait for Retry-After, then retry once
    - Iba pang errors: log warning, return None
    """
    session = get_session()
    url = f"{TMDB_BASE_URL}/{movie_id}"
    params = {"api_key": api_key}

    try:
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 10))
            logger.warning(f"Rate limited (429) sa movie {movie_id} — naghihintay ng {retry_after}s")
            time.sleep(retry_after)
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

        if response.status_code == 404:
            return None

        if response.status_code != 200:
            logger.warning(f"HTTP {response.status_code} para sa movie {movie_id} — skipping")
            return None

        data = response.json()

        budget = data.get("budget", 0) or 0
        revenue = data.get("revenue", 0) or 0

        # Genres: list of dicts → comma-separated string
        genres_list = data.get("genres", []) or []
        genres_str = ", ".join(g["name"] for g in genres_list if "name" in g)

        # Production countries: list of dicts → pipe-delimited "ISO:name" pairs
        countries_list = data.get("production_countries", []) or []
        countries_str = "|".join(
            f"{c.get('iso_3166_1', '')}:{c.get('name', '')}"
            for c in countries_list
            if c.get("iso_3166_1")
        )

        # Spoken languages: list of dicts → pipe-delimited "ISO:name" pairs
        languages_list = data.get("spoken_languages", []) or []
        languages_str = "|".join(
            f"{l.get('iso_639_1', '')}:{l.get('name', '')}"
            for l in languages_list
            if l.get("iso_639_1")
        )

        # Cast movie_id to int safely
        try:
            movie_id_int = int(movie_id)
        except (ValueError, TypeError):
            logger.warning(f"Invalid movie_id format: {movie_id} — skipping")
            return None

        return {
            "movie_id": movie_id_int,
            "budget": budget,
            "revenue": revenue,
            "genres": genres_str,
            "production_countries": countries_str,
            "spoken_languages": languages_str,
        }

    except requests.exceptions.RequestException as e:
        logger.warning(f"Connection error para sa movie {movie_id}: {e} — skipping")
        return None
    except Exception as e:
        logger.warning(f"Unexpected error para sa movie {movie_id}: {e} — skipping")
        return None


def main():
    """
    Main function — i-enrich ang movies mula sa Bronze gamit ang TMDB API.
    Steps: kunin candidates, tawagan API concurrently, i-save sa silver.movies_enriched.
    """
    logger.info("=== Simula ng Silver Enrichment ===")

    try:
        engine = get_engine()
        logger.info("Database connection established")

        # Load API keys — round-robin distribution
        api_key_1 = os.environ.get("TMDB_API_KEY_1")
        api_key_2 = os.environ.get("TMDB_API_KEY_2")
        api_key_3 = os.environ.get("TMDB_API_KEY_3")

        if not api_key_1:
            raise ValueError("TMDB_API_KEY_1 not found sa environment variables")

        api_keys = [api_key_1]
        if api_key_2 and api_key_2 not in ("<PASTE_YOUR_2ND_API_KEY_HERE>", ""):
            api_keys.append(api_key_2)
        if api_key_3 and api_key_3 not in ("<PASTE_YOUR_3RD_API_KEY_HERE>", ""):
            api_keys.append(api_key_3)

        logger.info(f"TMDB API keys loaded: {len(api_keys)} keys available")

        # Step 1: Kunin ang candidates
        candidate_ids = get_candidates(engine)
        total = len(candidate_ids)

        if total == 0:
            logger.info("Walang candidates para i-enrich — lahat ng fields ay kumpleto na")
            logger.info("=== Silver Enrichment TAPOS NA ===")
            return

        # Step 2: TRUNCATE silver.movies_enriched bago mag-insert (idempotent)
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE silver.movies_enriched"))
            conn.commit()
        logger.info("Na-truncate ang silver.movies_enriched")

        # Step 3: Tawagan ang TMDB API — 60 concurrent threads (3 keys × 20 workers)
        logger.info(f"Nagsisimula ng concurrent enrichment: {MAX_WORKERS} threads, {total} candidates")
        enriched_results = []
        skipped = 0
        api_calls = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(call_tmdb_api, movie_id, get_next_api_key(api_keys)): movie_id
                for movie_id in candidate_ids
            }

            for i, future in enumerate(as_completed(futures), 1):
                api_calls += 1
                result = future.result()

                if result is not None:
                    # Keep row kung may kahit isang useful field
                    if (result["budget"] > 0 or result["revenue"] > 0
                            or result["genres"]
                            or result["production_countries"]
                            or result["spoken_languages"]):
                        enriched_results.append(result)
                    else:
                        skipped += 1
                else:
                    skipped += 1

                if i % PROGRESS_INTERVAL == 0:
                    pct = (i / total) * 100
                    logger.info(f"Progress: {i}/{total} movies ({pct:.1f}%) — enriched so far: {len(enriched_results)}")

        # Step 4: I-save ang results sa silver.movies_enriched
        logger.info(f"API calls tapos na. Enriched: {len(enriched_results)}, Skipped: {skipped}")

        if enriched_results:
            df = pd.DataFrame(enriched_results)
            df.to_sql(
                name="movies_enriched",
                schema="silver",
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000,
            )
            logger.info(f"Na-insert ang {len(df)} enriched rows sa silver.movies_enriched")

        # Step 5: Verify row count
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM silver.movies_enriched"))
            db_count = result.scalar()

        logger.info(f"Verification: silver.movies_enriched = {db_count} rows")

        assert db_count > 0, (
            "Zero rows na-enrich mula sa TMDB API — "
            "baka may problema sa API key o network connectivity"
        )

        logger.info("=== Silver Enrichment SUMMARY ===")
        logger.info(f"  Total candidates queried: {total}")
        logger.info(f"  Successfully enriched (written to table): {len(enriched_results)}")
        logger.info(f"  Skipped (404/error/no useful data): {skipped}")
        logger.info(f"  API calls made: {api_calls}")
        logger.info("=== Silver Enrichment TAPOS NA ===")

    except Exception as e:
        logger.error(f"Error sa Silver Enrichment: {e}")
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        sys.exit(1)
