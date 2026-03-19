"""
Movie Pipeline DAG — Medallion Architecture (Bronze → Silver → Gold)

Ito ang IISANG DAG para sa buong movie data pipeline.
Manually triggered — walang schedule, i-trigger lang kapag kailangan.

Layers:
- Bronze: Raw data mula sa GCS bucket, lahat TEXT, walang transforms
- Silver: Typed tables + TMDB API enrichment para sa missing budget/revenue/genres
- Gold: (to be added) Business-ready aggregations gamit dbt

Bawat layer ay naka-TaskGroup para organized sa Airflow UI.
Kung mag-fail ang isang task, hihinto ang buong pipeline — fail loudly.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

# === Default Args ===
# Walang retries — gusto nating makita ang totoong error agad-agad
# depends_on_past=False kasi manual trigger, walang historical dependency
default_args = {
    "owner": "capstone",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

# === DAG Definition ===
# schedule_interval=None kasi manually triggered lang
# max_active_runs=1 para hindi magka-overlap ang runs (prevents data corruption)
# catchup=False kasi walang schedule na kailangan i-backfill
with DAG(
    dag_id="movie_pipeline",
    description="Medallion pipeline: Bronze → Silver → Gold para sa movie dataset",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["capstone", "medallion", "movies"],
    doc_md=__doc__,
) as dag:

    # =============================================================
    # BRONZE LAYER
    # Raw data mula sa GCS → Postgres bronze schema
    # Lahat ng columns TEXT, walang transformation
    # =============================================================
    with TaskGroup("bronze_tasks") as bronze_tasks:

        # Task 1: Gawa ng bronze schema at tables kung wala pa
        # Lahat ng columns ay TEXT, bawat column may COMMENT
        # IF NOT EXISTS — safe i-rerun kahit existing na ang tables
        ddl_bronze = BashOperator(
            task_id="ddl_bronze",
            bash_command="docker exec pandas-worker python /scripts/bronze/bronze_ddl.py",
            doc="Gawa ng bronze schema at tables. All columns TEXT, with COMMENTs.",
        )

        # Task 2: Basa ng CSVs mula GCS bucket, TRUNCATE + INSERT sa bronze tables
        # dtype=str para lahat ng values ay raw string — walang type inference
        # TRUNCATE muna bago INSERT para idempotent (safe i-rerun)
        load_bronze = BashOperator(
            task_id="load_bronze",
            bash_command="docker exec pandas-worker python /scripts/bronze/bronze_load.py",
            doc="Basa ng CSVs mula GCS bucket, TRUNCATE + INSERT sa bronze tables. "
                "Lahat ng columns naka-TEXT, walang transforms.",
        )

        # Task 3: Pandera validation, row count check vs GCS source, NULL id check
        # Kung may failure, mag-exit(1) ang script — Airflow catches non-zero exit
        # Pipeline stops dito kung hindi valid ang Bronze data
        validate_bronze = BashOperator(
            task_id="validate_bronze",
            bash_command="docker exec pandas-worker python /scripts/bronze/bronze_validate.py",
            doc="Pandera schema validation, row count check vs GCS source, NULL id check. "
                "Kung may failure, pipeline stops. Hindi tayo magpo-proceed sa Silver "
                "kung hindi validated ang Bronze.",
        )

        # Bronze task dependencies — linear, strict ordering
        # Walang parallelism — bawat step depends sa previous
        ddl_bronze >> load_bronze >> validate_bronze

    # =============================================================
    # SILVER LAYER
    # Typed tables + TMDB API enrichment para sa missing data
    # transform_silver at validate_silver will be added sa next step
    # =============================================================
    with TaskGroup("silver_tasks") as silver_tasks:

        # Task 1: Gawa ng silver schema at typed tables kung wala pa
        # INTEGER, DATE, NUMERIC columns — hindi na lahat TEXT tulad ng Bronze
        # IF NOT EXISTS — safe i-rerun kahit existing na ang tables
        ddl_silver = BashOperator(
            task_id="ddl_silver",
            bash_command="docker exec pandas-worker python /scripts/silver/silver_ddl.py",
            doc="Gawa ng silver schema at typed tables (movies_main, movie_extended, "
                "movies_enriched). With COMMENTs.",
        )

        # Task 2: TMDB API enrichment para sa movies na may missing budget/revenue/genres
        # Binabasa ang bronze rows na may 0/NULL values, tinatawagan ang TMDB API
        # Results na-save sa silver.movies_enriched — supplement table, hindi replacement
        # ~38K candidates at 20 req/sec = ~30 minutes runtime — normal ito
        enrich_silver = BashOperator(
            task_id="enrich_silver",
            bash_command="docker exec pandas-worker python /scripts/silver/silver_enrich.py",
            doc="TMDB API enrichment: basahin ang bronze rows na may missing "
                "budget/revenue/genres, tawagin ang TMDB API, at i-save ang "
                "results sa silver.movies_enriched.",
        )

        # transform_silver at validate_silver will be added sa next step
        ddl_silver >> enrich_silver

    # =============================================================
    # GOLD LAYER (to be added)
    # =============================================================
    # with TaskGroup('gold_tasks') as gold_tasks:
    #     dbt_run = BashOperator(...)
    #     dbt_test = BashOperator(...)
    #     dbt_run >> dbt_test

    # =============================================================
    # CROSS-LAYER DEPENDENCIES
    # Bronze must pass validation bago mag-start ang Silver
    # =============================================================
    bronze_tasks >> silver_tasks

    # bronze_tasks >> silver_tasks >> gold_tasks  # buong chain kapag kumpleto na
