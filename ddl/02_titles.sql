CREATE TABLE IF NOT EXISTS imdb.titles
(
    tconst String,
    title_type LowCardinality(String),
    primary_title String,
    original_title String,
    start_year Nullable(UInt16),
    end_year Nullable(UInt16),
    runtime_minutes Nullable(UInt32),
    genres String,
    primary_genre LowCardinality(String),
    average_rating Nullable(Float32),
    num_votes Nullable(UInt32)
)
ENGINE = MergeTree()
PARTITION BY ifNull(start_year, 0)
ORDER BY (title_type, primary_genre, tconst)
SETTINGS index_granularity = 8192;
