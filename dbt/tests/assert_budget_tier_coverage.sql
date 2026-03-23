-- assert_budget_tier_coverage
-- Every row in int_movie_financials must have a non-NULL budget_tier.
-- The classify_budget macro always returns a value (Unknown for NULLs),
-- so no row should ever have a NULL tier.
-- Returns rows with NULL budget_tier — expect 0.

SELECT movie_id, movie_title, budget, budget_tier
FROM {{ ref('int_movie_financials') }}
WHERE budget_tier IS NULL
