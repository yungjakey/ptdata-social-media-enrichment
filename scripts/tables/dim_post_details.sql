-- Create dimensions table for post details
CREATE TABLE IF NOT EXISTS dev_gold.dim_post_details (
    id BINARY,
    post_key BINARY,
    hashtag_key BINARY,
    last_update_time TIMESTAMP,
    post_link STRING,
    image_link STRING,
    post_type STRING,
    post_attachment_caption STRING,
    post_attachment_name STRING,
    post_attachment_description STRING,
    post_message STRING
)
LOCATION 's3://aws-orf-social-media-analytics/dev/gold/dim/dim_post_details/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET',
    'write_compression' = 'snappy',
    'vacuum_min_snapshots_to_keep' = '10',
    'vacuum_max_snapshot_age_seconds' = '604800'
);
