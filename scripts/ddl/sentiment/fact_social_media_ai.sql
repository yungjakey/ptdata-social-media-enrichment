-- Create facts table for AI-generated social media metrics
CREATE TABLE IF NOT EXISTS dev_gold.fact_social_media_ai (
    id BINARY,
    date_key BINARY,
    post_key BINARY,
    evaluation_time TIMESTAMP,
    last_update_time TIMESTAMP,
    sentiment_score DOUBLE,
    engagement_score DOUBLE,
    reach_score DOUBLE,
    influence_score DOUBLE,
    overall_impact DOUBLE,
    confidence DOUBLE
)
PARTITIONED BY (month(evaluation_time))
LOCATION 's3://aws-orf-social-media-analytics/dev/gold/fact/social_media/ai'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET',
    'write_compression' = 'snappy',
    'vacuum_min_snapshots_to_keep' = '10',
    'vacuum_max_snapshot_age_seconds' = '604800'
);