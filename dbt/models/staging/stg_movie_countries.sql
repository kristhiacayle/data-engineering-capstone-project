{{ config(materialized='view') }}

-- stg_movie_countries
-- One row per movie-country pair, scoped to 1980-2015.
-- Includes region, subregion, and service restriction flag.

SELECT
    pc.movie_id,
    pc.iso_country_code,
    pc.country_name,
    pc.country_region,
    pc.country_subregion,
    pc.is_service_restricted
FROM {{ source('silver', 'producing_countries') }} pc
INNER JOIN {{ ref('stg_movies') }} m ON m.movie_id = pc.movie_id
