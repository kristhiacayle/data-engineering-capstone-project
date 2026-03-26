# New Device Setup and Pipeline Run

This guide is for a first-time setup on a different laptop or desktop.

## 1. Get the Repository Onto the New Machine

Option A, clone with Git:

```powershell
git clone <your-repo-url>
cd data-engineering-capstone-project
```

Option B, copy the project folder manually:

- Copy the full project directory to the new machine
- Open it in a terminal or editor

## 2. Start Docker Desktop

Before running any commands:

- Open Docker Desktop
- Wait until Docker shows that it is running

If Docker is not running, `docker-compose up` will fail.

## 3. Place the GCS Service Account Key

Create this file path if it does not exist yet:

```text
secrets/gcs_key.json
```

Paste the downloaded service account JSON key into that file.

## 4. Create the `.env` File

Create `.env` in the project root with values like this:

```dotenv
# === PostgreSQL ===
POSTGRES_USER=capstone
POSTGRES_PASSWORD=capstone123
POSTGRES_DB=movies_pipeline

# === Port Mappings ===
POSTGRES_PORT=5433
AIRFLOW_PORT=8080

# === Airflow ===
AIRFLOW_FERNET_KEY=<your_fernet_key>

# === TMDB API Keys ===
TMDB_API_KEY_1=<your_key_1>
TMDB_API_KEY_2=<your_key_2>
TMDB_API_KEY_3=<your_key_3>
```

To generate the Airflow Fernet key:

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## 5. Build and Start the Containers

From the project root:

```powershell
docker-compose up --build -d
```

## 6. Verify That the Containers Are Running

Run:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

You should see these containers:

- `postgres`
- `airflow-init`
- `airflow-webserver`
- `airflow-scheduler`
- `pandas-worker`
- `dbt-model`

Notes:

- `airflow-init` is expected to finish and exit after setup
- the others should remain running

## 7. Open Airflow and Trigger the Pipeline

Go to:

```text
http://localhost:8080
```

Login with:

- username: `admin`
- password: `admin`

Find the DAG:

```text
movie_pipeline
```

Run it manually:

1. Turn the DAG on if needed
2. Click the play button or trigger button
3. Wait for all Bronze, Silver, and Gold tasks to succeed

## 8. Optional Manual Run Fallback

If Airflow is unavailable and you need to run pieces manually, use the existing project commands documented in `README.md` under:

- `Running the Pipeline`
- `dbt Gold Layer`

Airflow is still the preferred path for a full reproduction.

## 9. Validate That Gold Was Rebuilt

Use DBeaver or `psql` to validate the warehouse.

Recommended checks:

```sql
select count(*) as total_rows, max(release_year) as max_year
from gold.fact_movies;

select count(*) filter (where release_year = 2016) as rows_2016
from gold.fact_movies;

select count(*) as total_rows, max(release_year) as max_year
from gold.mart_yearly_trends;
```

Expected outcomes:

- `gold.fact_movies` total rows: `33054`
- `gold.fact_movies` max year: `2016`
- `gold.mart_yearly_trends` max year: `2016`

## 10. Move to Power BI Only After Gold Is Correct

Do not open or refresh Power BI first.

The correct order is:

1. containers up
2. Airflow run complete
3. `gold` validated
4. Power BI refresh

Continue with [Power BI Refresh and Dashboard Rebuild](03_power_bi_refresh_and_dashboard_rebuild.md).
