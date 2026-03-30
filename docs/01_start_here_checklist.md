# Start Here: New-Device Reproducibility Checklist

Use this checklist when moving the project from one machine to another.

## Goal

Get the full stack running end-to-end on a new device:

1. Docker services start successfully
2. Airflow can run the `movie_pipeline` DAG
3. PostgreSQL is populated through `gold`
4. Power BI refreshes against the correct `gold` schema
5. The dashboard shows the expected 1980-2016 numbers

## Software to Install First

- Git
- Docker Desktop
- Power BI Desktop
- DBeaver or another SQL client, optional but strongly recommended
- VS Code or another editor, optional

## Files and Access You Must Have Before Starting

- A copy or clone of this repository
- GCS service account key with access to bucket `internship-capstone-movies`
- Three TMDB API v3 keys
- Access to the final PBIX file used for presentation
- Git access if you need to pull or push from the new machine

## Files You Need to Place Manually

- `secrets/gcs_key.json`
- `.env`

Do not commit either file.

## Default Local Ports

- PostgreSQL: `localhost:5433`
- Airflow UI: `http://localhost:8080`

## Default Airflow Login

- Username: `admin`
- Password: `admin`

## Recommended Reproduction Order

1. Clone or copy the repository to the new machine
2. Install Docker Desktop and confirm it is running
3. Place `secrets/gcs_key.json`
4. Create `.env`
5. Run `docker-compose up --build -d`
6. Confirm all containers are healthy
7. Trigger the `movie_pipeline` DAG in Airflow
8. Validate `bronze`, `silver`, and `gold` tables
9. Open `movie_capstone.pbix`
10. Confirm all Power Query imports point to `gold`, not `gold_gold`
11. Refresh Power BI and validate the Page 1 title, subtitle, and KPI cards

## Expected Final Validation Numbers

After a successful rebuild and Power BI refresh, the default unfiltered Page 1 state should show:

- Title: `Movie Catalog • 1980–2016`
- Subtitle: `Showing: All languages • All genres`

- Total films: `33,054`
- `% English Titles`: `68.86%`
- `Top Genre Share - Drama`: `46.08%`
- Max release year in scope: `2016`

Page 1 should also behave dynamically:

- the two-line title should change when page filters change
- all three KPI values should change when page filters change

If those do not match, use [Validation and Troubleshooting](04_validation_and_troubleshooting.md).
