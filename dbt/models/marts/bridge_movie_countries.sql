{{ config(materialized='table') }}

-- bridge_movie_countries
-- Many-to-many bridge: movie ↔ producing country.
-- Used for the 3-part country filter in Power BI:
--   country (search + multiselect), region/subregion (dropdown), is_service_restricted (tickbox).

SELECT
    movie_id,
    iso_country_code,
    country_name,
    country_region,
    country_subregion,
    is_service_restricted
FROM {{ ref('stg_movie_countries') }}
