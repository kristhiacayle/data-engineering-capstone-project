-- assert_scope_years_valid
-- All movies in stg_movies must be within the 1980-2015 scope window.
-- Returns rows that violate the scope filter — expect 0.

SELECT movie_id, movie_title, release_year
FROM {{ ref('stg_movies') }}
WHERE release_year < 1980
   OR release_year > 2015
