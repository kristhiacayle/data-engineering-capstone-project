{{ config(materialized='table') }}

-- mart_yearly_trends
-- Year-over-year movie production trends for Power BI line/bar charts.
-- Grain: one row per release year (1980-2015).
-- Sources from int_yearly_movie_trends which contains LAG/LEAD window functions.

SELECT
    release_year,
    movie_count,
    movies_with_budget,
    movies_with_revenue,
    avg_budget,
    avg_revenue,
    median_budget,
    median_revenue,
    prev_year_movie_count,
    next_year_movie_count,
    yoy_movie_count_delta,
    yoy_movie_count_pct,
    rolling_3yr_avg_movies
FROM {{ ref('int_yearly_movie_trends') }}
ORDER BY release_year
