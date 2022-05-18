CREATE TABLE IF NOT EXISTS core.%s 
(
    date Date DEFAULT toDate(0),
    name String,
    tags Array(String),
    val Float64,
    ts DateTime,
    updated DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS index_granularity = 8192;