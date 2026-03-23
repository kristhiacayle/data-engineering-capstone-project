{{ config(materialized='table') }}

-- fact_movies
-- Grain: one row per movie (movie_title).
-- Core movie metrics with pre-aggregated counts for Power BI.
-- Scoped to 1980-2015 via stg_movies.

SELECT
    m.movie_id,
    m.movie_title,
    m.release_date,
    m.release_year,
    m.budget,
    m.revenue,

    -- Pre-aggregated counts (avoids runtime aggregation in Power BI)
    COALESCE(g.genre_count, 0)      AS genre_count,
    COALESCE(c.company_count, 0)    AS company_count,
    COALESCE(co.country_count, 0)   AS country_count,
    COALESCE(l.language_count, 0)   AS language_count

FROM {{ ref('stg_movies') }} m

LEFT JOIN (
    SELECT movie_id, COUNT(DISTINCT genre) AS genre_count
    FROM {{ ref('stg_movie_genres') }}
    GROUP BY movie_id
) g ON g.movie_id = m.movie_id

LEFT JOIN (
    SELECT movie_id, COUNT(*) AS company_count
    FROM {{ ref('stg_movie_companies') }}
    GROUP BY movie_id
) c ON c.movie_id = m.movie_id

LEFT JOIN (
    SELECT movie_id, COUNT(DISTINCT iso_country_code) AS country_count
    FROM {{ ref('stg_movie_countries') }}
    GROUP BY movie_id
) co ON co.movie_id = m.movie_id

LEFT JOIN (
    SELECT movie_id, COUNT(*) AS language_count
    FROM {{ ref('stg_movie_languages') }}
    GROUP BY movie_id
) l ON l.movie_id = m.movie_id
