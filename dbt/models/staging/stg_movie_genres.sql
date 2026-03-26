{{ config(materialized='view') }}

-- stg_movie_genres
-- One row per movie-genre pair, scoped to 1980-2016.
-- DISTINCT on (movie_id, genre) to eliminate upstream duplicate genre rows
-- (known data quality issue: some movies have genres written twice in bronze).

SELECT DISTINCT
    g.movie_id,
    g.genre
FROM {{ source('silver', 'movie_genres') }} g
INNER JOIN {{ ref('stg_movies') }} m ON m.movie_id = g.movie_id
