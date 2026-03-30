# Power BI Refresh and Dashboard Rebuild

This guide explains how to get the PBIX working correctly on a new machine without redoing the report design from scratch.

## 1. Open the Correct PBIX

Use the final presentation file currently maintained for the dashboard.

Recommended file:

```text
movie_capstone.pbix
```

If your team later renames the PBIX, use the latest finalized version instead.

## 2. Confirm the Database Connection

In Power BI Desktop:

1. Open `Transform data`
2. Go to `Home -> Data source settings`
3. Confirm the source points to:
   - Server: `localhost:5433`
   - Database: `movies_pipeline`

## 3. Confirm the Imported Tables Point to `gold`, Not `gold_gold`

This is the most important refresh check.

In Power Query, for every imported warehouse table used by the dashboard, confirm the `Navigation` step uses:

```powerquery
Source{[Schema="gold", Item="<table_name>"]}[Data]
```

and not:

```powerquery
Source{[Schema="gold_gold", Item="<table_name>"]}[Data]
```

Key tables to verify:

- `fact_movies`
- `bridge_movie_companies`
- `bridge_movie_countries`
- `bridge_movie_genres`
- `bridge_movie_languages`
- `mart_country_summary`
- `mart_genre_share`
- `mart_language_share`
- `mart_yearly_trends`

## 4. Remove Any Temporary Query Steps Left Over From Debugging

If you see Power Query steps like:

- `Filtered Rows`
- `Sorted Rows`

remove them unless they were intentionally designed into the model.

These steps can make the imported data look incomplete.

## 5. Refresh Power Query Properly

Inside Power Query:

1. Click `Refresh Preview`
2. Wait for the preview to finish loading
3. Click `Close & Apply`

Then back in the main Power BI window:

1. Click `Home -> Refresh`

## 6. Validate the Main Dashboard Numbers

After refresh, clear Page 1 slicers back to the default state and confirm the report shows:

- Title: `Movie Catalog • 1980–2016`
- Subtitle: `Showing: All languages • All genres`

- Total films: `33,054`
- `% English Titles`: `68.86%`
- `Top Genre Share - Drama`: `46.08%`

If you still see:

- `31,450`
- max year `2015`

then the PBIX is still pointing to stale data or the wrong schema.

## 7. Static Tables and PBIX-Only Objects

The current dashboard also uses PBIX-side objects such as:

- `GenreDemandWeights`
- `AcquisitionPriority`
- dynamic DAX measures for interactive visuals
- dynamic Page 1 title/subtitle measures:
  - `Movie Catalog Title (Dynamic)`
  - `Movie Catalog Subtitle (Dynamic)`
- dynamic Page 1 KPI measures:
  - `Total Films in Catalog (Dynamic Display)`
  - `% English Titles (Dynamic)`
  - `Top Genre Share % (Dynamic)`
  - `Top Genre Share Label (Dynamic)`

These should already come with the PBIX file. You should not need to recreate them manually if you are opening the existing finalized PBIX.

## 8. Confirm Final Page 1 Behavior

Page 1 should be dynamic under the global page filters.

When the following slicers change:

- release date
- production geography
- `is_service_restricted`
- spoken language
- genre

the report should update:

- all Page 1 charts
- all three KPI values
- the dynamic title line
- the dynamic subtitle line
- the top-genre KPI label

The one exception is the default title/subtitle text shown only when no slicer narrowing is active:

- `Movie Catalog • 1980–2016`
- `Showing: All languages • All genres`

## 9. If Visuals Look Wrong After Refresh

Check for these common causes:

- the query still points to `gold_gold`
- Power Query preview did not truly refresh
- the PBIX was opened before the pipeline finished
- a visual is bound to an old field after a model edit

If needed, continue with [Validation and Troubleshooting](04_validation_and_troubleshooting.md).
