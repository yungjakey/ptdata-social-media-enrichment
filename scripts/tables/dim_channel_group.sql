-- Create dimensions table for channel groups
CREATE TABLE IF NOT EXISTS dev_gold.dim_channel_group (
    id BINARY,
    group_key BINARY,
    group_name STRING,
    last_update_time TIMESTAMP
)
LOCATION 's3://aws-orf-social-media-analytics/dev/gold/dim/dim_channel_group/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET',
    'write_compression' = 'snappy',
    'vacuum_min_snapshots_to_keep' = '10',
    'vacuum_max_snapshot_age_seconds' = '604800'
);
