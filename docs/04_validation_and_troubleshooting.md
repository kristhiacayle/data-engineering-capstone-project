# Validation and Troubleshooting

Use this guide when the pipeline or dashboard does not match the expected 1980-2016 state.

## A. Quick Validation Checklist

The reproduction is successful only if all of these are true:

- Docker containers start successfully
- Airflow DAG `movie_pipeline` finishes successfully
- `gold.fact_movies` has `33,054` rows
- `gold.fact_movies` max `release_year` is `2016`
- Power BI shows:
  - `Movie Catalog • 1980–2016`
  - `Showing: All languages • All genres`
  - `33,054`
  - `68.86%`
  - `46.08%`

## B. SQL Checks to Run in DBeaver

### Check gold fact scope

```sql
select
    count(*) as total_rows,
    count(*) filter (where release_year = 2016) as rows_2016,
    min(release_year) as min_year,
    max(release_year) as max_year
from gold.fact_movies;
```

Expected:

- `total_rows = 33054`
- `rows_2016 = 1604`
- `min_year = 1980`
- `max_year = 2016`

### Check yearly trends scope

```sql
select
    count(*) as total_rows,
    min(release_year) as min_year,
    max(release_year) as max_year
from gold.mart_yearly_trends;
```

Expected:

- `total_rows = 37`
- `min_year = 1980`
- `max_year = 2016`

## C. If Power BI Still Shows 2015

Most common causes:

1. The Power Query `Navigation` step still points to `gold_gold`
2. Power Query preview is stale
3. `Close & Apply` was not completed after refresh
4. The PBIX was refreshed before the Airflow run finished

Fix:

1. Open `Transform data`
2. Check every imported warehouse table
3. Change `Schema="gold_gold"` to `Schema="gold"` if needed
4. Click `Refresh Preview`
5. Click `Close & Apply`
6. Click `Home -> Refresh`

## D. If the Airflow DAG Fails

Check:

- Docker Desktop is running
- `.env` exists in the project root
- `secrets/gcs_key.json` exists
- TMDB API keys are populated
- `postgres` is healthy before the DAG starts

Useful commands:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
docker logs airflow-webserver --tail 100
docker logs airflow-scheduler --tail 100
docker logs postgres --tail 100
```

## E. If PostgreSQL Does Not Have the Expected Tables

Run:

```sql
select table_schema, table_name
from information_schema.tables
where table_schema in ('bronze', 'silver', 'gold', 'gold_gold')
order by table_schema, table_name;
```

What to look for:

- `gold` should contain the dbt-managed models
- `gold_gold` may still exist from old Power BI import history, but it should not be the active schema used by the refreshed PBIX

## F. If the Dashboard Numbers Are Wrong but SQL Is Right

That usually means the problem is in Power BI, not the warehouse.

Check:

- data source settings
- Power Query `Navigation` step
- stale preview
- old imported model cache

## G. Final Sanity Checks Before Presentation

Page 1 should show:

- title: `Movie Catalog • 1980–2016`
- subtitle: `Showing: All languages • All genres`
- `33,054` films
- `% English Titles = 68.86%`
- `Top Genre Share - Drama = 46.08%`

Page 1 should also respond to the global page filters:

- title year should react to the release-date slider
- title geography should react to production geography filters
- subtitle should react to language and genre slicers
- all three KPI values should change with the active filter context
- the third KPI label should update to the current top genre

Page 2 should show:

- `Action` as the largest positive genre gap
- `Drama` as overrepresented
- acquisition table top rows led by:
  - Spanish
  - Hindi
  - Arabic

## H. What Not to Commit

Do not commit these local-only files:

- `.env`
- `secrets/gcs_key.json`
- PBIX files unless intentionally versioning them
- local scratch folders such as `.claude/`

## I. If the Page 1 Title Year Does Not Move With the Date Slider

Most likely cause:

- the title measure is reading `release_year` instead of `release_date`

Expected fix in the finalized PBIX:

- `Movie Catalog Title (Dynamic)` derives the year text from `gold_gold fact_movies[release_date]`

If the DAX is already correct but the canvas still shows the old title:

1. click the title card visual
2. remove `Movie Catalog Title (Dynamic)` from the field well
3. drag it back in

Sometimes the card keeps stale rendered text until it is rebound.
