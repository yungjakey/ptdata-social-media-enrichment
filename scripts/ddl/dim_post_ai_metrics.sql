-- Create dimensions table for AI-generated post details
CREATE TABLE IF NOT EXISTS dev_gold.dim_ai_metrics (
    id BINARY
    post_key BINARY,
    evaluation_time TIMESTAMP,
    key_topics ARRAY<STRING>,
    content_category STRING,
    audience_type STRING,
    tone STRING,
    recommendations ARRAY<STRING>
)
LOCATION 's3://aws-orf-social-media-analytics/dev/gold/dim/ai_metrics'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET',
    'write_compression' = 'snappy',
    'vacuum_min_snapshots_to_keep' = '10',
    'vacuum_max_snapshot_age_seconds' = '604800'
);
