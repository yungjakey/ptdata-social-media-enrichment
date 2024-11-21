-- Create dimensions table for channel information
CREATE TABLE IF NOT EXISTS dev_gold.dim_channel (
    id BINARY,
    business_key BIGINT,
    channel_key BINARY,
    group_key BINARY,
    last_update_time TIMESTAMP,
    channel_name STRING,
    platform_name STRING,
    channel_description STRING
)
LOCATION 's3://aws-orf-social-media-analytics/dev/gold/dim/dim_channel/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET',
    'write_compression' = 'snappy',
    'vacuum_min_snapshots_to_keep' = '10',
    'vacuum_max_snapshot_age_seconds' = '604800'
);
