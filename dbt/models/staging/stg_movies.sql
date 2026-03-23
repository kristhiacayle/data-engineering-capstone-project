{{ config(materialized='view') }}

-- stg_movies
-- Base movie records scoped to 1980-2015.
-- This is the scope filter for the entire Gold layer —
-- all downstream models join to this to inherit the window.

SELECT
    movie_id,
    movie_title,
    release_date,
    EXTRACT(YEAR FROM release_date)::INTEGER AS release_year,
    budget,
    revenue
FROM {{ source('silver', 'movies') }}
WHERE release_date >= '1980-01-01'
  AND release_date < '2016-01-01'
