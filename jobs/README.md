# Lambda Jobs

This directory contains AWS Lambda functions for processing social media data, with a focus on sentiment analysis.

## Architecture

### Core Components

#### `lambda_handler.py`
- Generic Lambda handler for all job types
- Dynamic job loading based on URL path
- Secrets management for OpenAI credentials
- Error handling and response formatting

#### Job Structure
Each job type (e.g., `sentiment/`) contains:
```
jobs/
└── sentiment/
    ├── config.yaml    # Job-specific configuration
    └── main.py        # Job implementation
```

## Sentiment Analysis Job

### Implementation (`sentiment/main.py`)
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
        
        # Join and write results
        results = records.select([connector.config.target.index_field])
                        .join(results, keys=connector.config.target.index_field)
        await connector.write(records=results, model=provider.model)
```

### Configuration (`sentiment/config.yaml`)
```yaml
connector:
  warehouse: s3://aws-orf-social-media-analytics/dev/gold
  
  source:
    tables:
      - database: dev_gold
        table: fact_social_media_reaction_post
        datetime_field: last_update_time
        index_field: post_key
      - database: dev_gold
        table: dim_post_details
        datetime_field: last_update_time
        index_field: post_key
    time_filter_hours: 300
    max_records: 30  # Batch size
    
  target:
    database: dev_test
    table: social_media_sentiment
    datetime_field: written_at
    index_field: post_key
    partition_by:
      - year

inference:
  workers: 20
  response_format: sentiment
  exclude_fields:
    - post_key
    - hashtag_key
    - date_key
    - channel_key
    - last_update_time
```

## Lambda Integration

### URL Pattern
```
POST /sentiment?drop=false
```

### Environment Variables
- `OPENAI_SECRET_NAME`: Name of AWS Secrets Manager secret
- `AWS_REGION`: AWS region (default: eu-central-1)

### Secret Structure
```json
{
    "OPENAI_API_KEY": "sk-...",
    "OPENAI_API_BASE": "https://..."
}
```

## Features

### Dynamic Job Loading
```python
# Load job-specific module and config
module = import_module(f"jobs.{model_type}.main")
config = load_config(model_type)
main_func = module.main
```

### Error Handling
- Configuration validation
- AWS service errors
- Processing errors
- HTTP status codes

### Resource Management
- Signal handling for graceful shutdown
- Async context managers
- Proper cleanup

## Development

### Local Testing
```bash
# Run locally
cd jobs/sentiment
python main.py

# Test with SAM
sam local invoke -e events/sentiment.json
```

### Deployment
```bash
# Deploy specific function
sam deploy --template-file template.yaml

# Update configuration
aws s3 cp config.yaml s3://bucket/config/
```

## Best Practices

1. **Configuration**
   - Use YAML for readability
   - Validate with Pydantic
   - Separate source/target configs

2. **Error Handling**
   - Proper exception types
   - Detailed error messages
   - Status code mapping

3. **Performance**
   - Batch processing
   - Concurrent workers
   - Resource cleanup

4. **Security**
   - Secrets management
   - IAM roles
   - Input validation
