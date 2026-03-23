{{ config(materialized='table') }}

-- mart_genre_share
-- Genre % share: what % of scoped movies belong to each genre.
-- Use this for genre distribution charts in Power BI, not raw counts.
-- total_movies = distinct movies in scope with at least one genre.

WITH total AS (
    SELECT COUNT(DISTINCT movie_id) AS total_movies
    FROM {{ ref('stg_movie_genres') }}
)
SELECT
    g.genre,
    COUNT(DISTINCT g.movie_id)                                          AS movie_count,
    total.total_movies,
    {{ pct_of_total('COUNT(DISTINCT g.movie_id)', 'total.total_movies') }} AS pct_of_movies
FROM {{ ref('stg_movie_genres') }} g
CROSS JOIN total
GROUP BY g.genre, total.total_movies
ORDER BY movie_count DESC
