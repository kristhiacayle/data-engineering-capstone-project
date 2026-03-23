{{ config(materialized='view') }}

-- stg_movie_languages
-- One row per movie-language pair, scoped to 1980-2015.

SELECT
    sl.movie_id,
    sl.iso_language_code,
    sl.language_name
FROM {{ source('silver', 'spoken_languages') }} sl
INNER JOIN {{ ref('stg_movies') }} m ON m.movie_id = sl.movie_id
