-- assert_fact_counts_non_negative
-- Pre-aggregated count columns in fact_movies use COALESCE(..., 0),
-- so they must never be negative.
-- Returns rows with any negative count — expect 0.

SELECT movie_id, movie_title, genre_count, company_count, country_count, language_count
FROM {{ ref('fact_movies') }}
WHERE genre_count < 0
   OR company_count < 0
   OR country_count < 0
   OR language_count < 0
