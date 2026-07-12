CREATE TABLE IF NOT EXISTS imdb.akas
(
    title_id String,
    ordering UInt16,
    title String,
    region LowCardinality(String),
    language LowCardinality(String),
    types String,
    is_original_title UInt8
)
ENGINE = MergeTree()
PARTITION BY region
ORDER BY (title_id, ordering)
SETTINGS index_granularity = 8192;
