-- assert_fact_movies_no_orphan_bridges
-- Every movie_id in the bridge tables must exist in fact_movies.
-- Orphan bridge rows mean a movie passed the staging scope but not the fact table.
-- Returns orphan rows — expect 0.

SELECT 'bridge_movie_genres' AS bridge_table, movie_id
FROM {{ ref('bridge_movie_genres') }}
WHERE movie_id NOT IN (SELECT movie_id FROM {{ ref('fact_movies') }})

UNION ALL

SELECT 'bridge_movie_languages', movie_id
FROM {{ ref('bridge_movie_languages') }}
WHERE movie_id NOT IN (SELECT movie_id FROM {{ ref('fact_movies') }})

UNION ALL

SELECT 'bridge_movie_countries', movie_id
FROM {{ ref('bridge_movie_countries') }}
WHERE movie_id NOT IN (SELECT movie_id FROM {{ ref('fact_movies') }})

UNION ALL

SELECT 'bridge_movie_companies', movie_id
FROM {{ ref('bridge_movie_companies') }}
WHERE movie_id NOT IN (SELECT movie_id FROM {{ ref('fact_movies') }})
