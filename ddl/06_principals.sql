CREATE TABLE IF NOT EXISTS imdb.principals
(
    tconst String,
    ordering UInt16,
    nconst String,
    primary_name String,
    category LowCardinality(String),
    job String,
    characters String,
    title_type LowCardinality(String),
    primary_title String,
    primary_genre LowCardinality(String),
    birth_year Nullable(UInt16),
    primary_role LowCardinality(String)
)
ENGINE = MergeTree()
PARTITION BY category
ORDER BY (tconst, ordering, nconst)
SETTINGS index_granularity = 8192;
