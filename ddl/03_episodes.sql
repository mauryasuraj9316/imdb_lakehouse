CREATE TABLE IF NOT EXISTS imdb.episodes
(
    episode_tconst String,
    parent_tconst String,
    season_number Nullable(UInt16),
    episode_number Nullable(UInt16),
    episode_title String,
    series_title String,
    start_year Nullable(UInt16),
    primary_genre LowCardinality(String),
    average_rating Nullable(Float32),
    num_votes Nullable(UInt32),
    runtime_minutes Nullable(UInt32)
)
ENGINE = MergeTree()
PARTITION BY ifNull(start_year, 0)
ORDER BY (parent_tconst, ifNull(season_number, 0), ifNull(episode_number, 0))
SETTINGS index_granularity = 8192;
