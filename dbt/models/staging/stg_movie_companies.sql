{{ config(materialized='view') }}

-- stg_movie_companies
-- One row per movie-company pair, scoped to 1980-2016.

SELECT
    pc.movie_id,
    pc.company_name
FROM {{ source('silver', 'production_companies') }} pc
INNER JOIN {{ ref('stg_movies') }} m ON m.movie_id = pc.movie_id
