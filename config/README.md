# Lambda Jobs

This directory contains AWS Lambda functions for processing social media data, with a focus on sentiment analysis.

## Architecture

### Job Structure
Each job contains a config named accordingly:
```
config/
├── sentiment.yaml
└── ...
```

#### [`handler.py`](handler.py)
- Generic Lambda handler for all job types
- Dynamic job loading based on URL path
- Secrets management for OpenAI credentials
- Error handling and response formatting
- Triggered via either HTTP or scheduled event


#### [`main.py`](main.py)
- Generic entrypoint for all job types
- Handles reading and processing data
- Handles writing results to target location


### Configuration (`config/sentiment.yaml`)
```yaml
connector:
  source:
    tables:
    - database: prod_gold
      table: fact_social_media_reaction_post
      datetime_field: last_update_time
      index_field: post_key
    - database: prod_gold
      table: dim_post_details
      datetime_field: last_update_time
      index_field: post_key
    time_filter_hours: 300
    max_records: 10  # Process in batches
    
  target:
    database: dev_gold
    table: social_media_sentiment
    datetime_field: written_at
    index_field: post_key
    location: s3://aws-orf-social-media-analytics/dev/gold/ai/social_media_sentiment
    partition_by:
      - year
      - month
      - day

inference:
  workers: 20
  response_format: sentiment
  exclude_fields:
  - id  
  - post_key
  - hashtag_key
  - date_key
  - channel_key
  - last_update_time  
  - post_attachment_name  
  - post_link
  - image_link
```

### Implementation (`main.py`)
```python
async def main(config: RootConfig, drop: bool = False) -> None:
    connector = AWSConnector.from_config(config.connector)
    
    async with InferenceClient.from_config(config.inference) as provider:
        # Read source data
        records = await connector.read(drop=drop)
        
        # Process with OpenAI
        results = await provider.process_batch(
            records=records, 
            index=connector.config.target.index_field
        )
        await connector.write(records=results)
```

### Lambda Integration

#### URL Pattern
```
POST /{model_name}
```

#### Environment Variables
- `OPENAI_SECRET_NAME`: Name of AWS Secrets Manager secret

```json
{
    "OPENAI_API_KEY": "sk-...",
    "OPENAI_API_BASE": "https://..."
}
```

## Development

### Local Testing
```bash
# Run locally
python main.py --model sentiment

# Test with SAM
sam local invoke -e events/sentiment.json -n events/env.json
```


### Deployment
```bash
sam deploy
```
