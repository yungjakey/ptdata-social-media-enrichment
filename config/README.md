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

### Lambda Functions

#### `UserNeedsFunction`
- Scheduled Lambda function that runs every 30 minutes
- Processes social media data to extract user needs and insights
- Configured through SAM template and sentiment.yaml

#### `ModelInferenceFunction`
- HTTP API endpoint for on-demand model inference
- Supports different model types through path parameters
- IAM-authenticated via API Gateway

### Configuration

#### SAM Template (`template.yaml`)
```yaml
Resources:
  UserNeedsFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: handler.lambda_handler
      Events:
        Schedule:
          Type: Schedule
          Properties:
            Schedule: rate(30 minutes)
            Input:
              path: /user_needs
              queryStringParameters:
                max_records: 10000

  ModelInferenceFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: handler.lambda_handler
      Events:
        HttpApi:
          Type: HttpApi
          Properties:
            Path: /{model}
            Method: GET
```

#### Job Config (`sentiment.yaml`)
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

### Environment Variables
- `OPENAI_SECRET_NAME`: Name of AWS Secrets Manager secret
- `ENVIRONMENT`: Deployment environment (dev/prod)
- `PYTHONPATH`: Set to `/var/task` in Lambda environment

### Deployment

The functions are deployed using AWS SAM:
```bash
# Build and deploy
make deploy
```

This will:
1. Export dependencies from Poetry to requirements.txt
2. Build the Lambda functions with the correct Python environment
3. Deploy to AWS using CloudFormation

### Local Development

For local development and testing:
```bash
# Install dependencies
poetry install

# Run tests
poetry run pytest

# Format code
poetry run black .
poetry run isort .
