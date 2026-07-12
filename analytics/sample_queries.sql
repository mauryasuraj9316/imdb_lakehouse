-- Sample ClickHouse analytics queries for IMDb OLAP layer.
-- Run via: clickhouse-client --multiquery < analytics/sample_queries.sql
-- Or connect to http://localhost:8123

-- 1. Top 20 highest-rated movies (min 10k votes) by decade
SELECT
    toUInt32(floor(start_year / 10) * 10) AS decade,
    primary_title,
    start_year,
    average_rating,
    num_votes
FROM imdb.titles
WHERE title_type = 'movie'
  AND num_votes >= 10000
  AND average_rating IS NOT NULL
  AND start_year > 0
ORDER BY decade DESC, average_rating DESC
LIMIT 20;

-- 2. Average rating by title type
SELECT
    title_type,
    round(avg(average_rating), 2) AS avg_rating,
    count() AS title_count
FROM imdb.titles
WHERE average_rating IS NOT NULL
GROUP BY title_type
ORDER BY title_count DESC;

-- 3. Top 10 TV series by episode count with avg episode runtime
SELECT
    e.series_title,
    count() AS episode_count,
    round(avg(e.runtime_minutes), 1) AS avg_runtime_minutes
FROM imdb.episodes e
WHERE e.series_title IS NOT NULL
  AND e.series_title != ''
GROUP BY e.series_title
ORDER BY episode_count DESC
LIMIT 10;

-- 4. Genre popularity (last 20 years): count + avg rating
SELECT
    primary_genre,
    count() AS title_count,
    round(avg(average_rating), 2) AS avg_rating
FROM imdb.titles
WHERE start_year >= (toYear(today()) - 20)
  AND start_year > 0
  AND average_rating IS NOT NULL
GROUP BY primary_genre
ORDER BY title_count DESC
LIMIT 20;

-- 5. Runtime distribution: avg runtime by genre for movies
SELECT
    primary_genre,
    round(avg(runtime_minutes), 1) AS avg_runtime,
    quantile(0.5)(runtime_minutes) AS median_runtime,
    count() AS movie_count
FROM imdb.titles
WHERE title_type = 'movie'
  AND runtime_minutes IS NOT NULL
  AND runtime_minutes > 0
GROUP BY primary_genre
HAVING movie_count >= 100
ORDER BY avg_runtime DESC
LIMIT 20;

-- 6. Top 20 actors by number of titles
SELECT
    primary_name,
    count() AS title_count
FROM imdb.principals
WHERE category IN ('actor', 'actress')
GROUP BY primary_name
ORDER BY title_count DESC
LIMIT 20;

-- 7. Most common alternate title regions
SELECT
    region,
    count() AS aka_count
FROM imdb.akas
GROUP BY region
ORDER BY aka_count DESC
LIMIT 20;

-- 8. Top directors by movie count
SELECT
    primary_name,
    count() AS movie_count
FROM imdb.principals
WHERE category = 'director'
  AND title_type = 'movie'
GROUP BY primary_name
ORDER BY movie_count DESC
LIMIT 20;
