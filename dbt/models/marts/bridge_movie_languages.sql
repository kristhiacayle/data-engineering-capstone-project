{{ config(materialized='table') }}

-- bridge_movie_languages
-- Many-to-many bridge: movie ↔ spoken language.
-- Used for language multiselect filter and language % share in Power BI.

SELECT
    movie_id,
    iso_language_code,
    language_name
FROM {{ ref('stg_movie_languages') }}
