-- Create facts table for social media metrics
CREATE TABLE IF NOT EXISTS dev_gold.fact_social_media_reaction_post (
    id BINARY,
    date_key BINARY,
    channel_key BINARY,
    post_key BINARY,
    publishing_time TIMESTAMP,
    last_update_time TIMESTAMP,
    likes INT,
    love INT,
    wow INT,
    haha INT,
    sad INT,
    angry INT,
    thankful INT,
    comments INT,
    shares INT,
    reach INT,
    url_clicks INT,
    video_clicks INT,
    video_avg_time_watched INT,
    video_total_time_watched BIGINT,
    video_length INT, 
    video_unique_views INT,
    processed BOOLEAN
)
PARTITIONED BY (month(publishing_time))
LOCATION 's3://aws-orf-social-media-analytics/dev/gold/fact/fact_social_media_reaction_post/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET',
    'write_compression' = 'snappy',
    'vacuum_min_snapshots_to_keep' = '10',
    'vacuum_max_snapshot_age_seconds' = '604800'
);
