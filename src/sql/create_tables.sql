-- Create facts table for social media metrics
CREATE TABLE IF NOT EXISTS fact_social_media_ai_metrics (
    id STRING,
    source_id STRING,
    platform STRING,
    content_type STRING,
    content STRING,
    created_at TIMESTAMP,
    processed BOOLEAN,
    processed_at TIMESTAMP,
    sentiment DOUBLE,
    engagement_score DOUBLE,
    virality_score DOUBLE,
    topic_relevance DOUBLE,
    brand_safety_score DOUBLE,
    PRIMARY KEY (id) DISABLE NOVALIDATE
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 's3://ptdata-analytics/social-media/facts/';

-- Create dimensions table for social media details
CREATE TABLE IF NOT EXISTS dim_social_media_ai_details (
    id STRING,
    source_id STRING,
    topics ARRAY<STRING>,
    entities ARRAY<STRING>,
    keywords ARRAY<STRING>,
    summary STRING,
    language STRING,
    content_category STRING,
    brand_safety_categories ARRAY<STRING>,
    PRIMARY KEY (id) DISABLE NOVALIDATE
)
STORED AS PARQUET
LOCATION 's3://ptdata-analytics/social-media/dims/';
