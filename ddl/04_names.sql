CREATE TABLE IF NOT EXISTS imdb.names
(
    nconst String,
    primary_name String,
    birth_year Nullable(UInt16),
    death_year Nullable(UInt16),
    primary_profession String,
    primary_role LowCardinality(String),
    known_for_titles String
)
ENGINE = MergeTree()
PARTITION BY primary_role
ORDER BY (primary_name, nconst)
SETTINGS index_granularity = 8192;
