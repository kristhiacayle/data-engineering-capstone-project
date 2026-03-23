-- assert_service_restricted_countries_have_region
-- Countries marked is_service_restricted=TRUE must still have a valid geographic region.
-- Service restrictions are about platform access, not content origin —
-- restricted countries (CN, RU, KP, etc.) must retain their real region/subregion.
-- Returns restricted countries with NULL region — expect 0.

SELECT iso_country_code, country_name, country_region, country_subregion
FROM {{ ref('mart_country_summary') }}
WHERE is_service_restricted = TRUE
  AND (country_region IS NULL OR country_subregion IS NULL)
