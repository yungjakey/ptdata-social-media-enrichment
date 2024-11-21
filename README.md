# Social Media Analytics Enrichment

A serverless AWS Lambda function that enriches social media analytics data using OpenAI's language models. This service processes social media metrics and post details to generate AI-powered insights.

## Architecture

### Data Flow
1. **Input Data Sources**
   - Social media metrics from `fact_social_media_reaction_post` table
   - Post details from `dim_post_details` table

2. **Processing Pipeline**
   ```
   [RDS Tables] → [Lambda Function] → [OpenAI API] → [Enriched Data Tables]
   ```

3. **Output Data**
   - AI-enriched metrics in `fact_social_media_ai_metrics`
   - AI-enriched details in `dim_social_media_ai_details`

## Prerequisites

- Python 3.12+
- AWS Account with access to:
  - Lambda
  - RDS Data API
  - Secrets Manager
- Azure OpenAI API access

## Dependencies

```
boto3~=1.34.34    # AWS SDK for Python
openai~=1.54.4    # OpenAI API client
omegaconf~=2.3.0  # Configuration management
```

## Configuration

Create a `src/config.yaml` file:

```yaml
aws:
  secret_arn: "your-secret-arn"  # ARN of the secret containing OpenAI credentials

openai:
  api_version: "2024-02-15"
  engine: "your-deployment-name"
  temperature: 0.7
  max_tokens: 1000

tables:
  source:
    fact_social_media_reaction_post: "your_metrics_table"
    dim_post_details: "your_details_table"
  target:
    fact_social_media_ai_metrics: "your_ai_metrics_table"
    dim_social_media_ai_details: "your_ai_details_table"

models:
  post: |
    You are an AI analyst specialized in social media content.
    Analyze the provided metrics and details to generate insights.
    Return your analysis in the following JSON format:
    {
      "sentiment": "positive|negative|neutral",
      "engagement_score": 0-100,
      "content_category": "string",
      "key_topics": ["topic1", "topic2"],
      "recommendations": ["rec1", "rec2"]
    }
```

## AWS Secrets Manager

Create a secret with the following structure:
```json
{
  "OPENAI_API_KEY": "your-api-key",
  "OPENAI_API_BASE": "your-azure-endpoint"
}
```

## Database Schema

### Input Tables

#### fact_social_media_reaction_post
- id: bytes (primary key)
- post_key: string
- date_key: datetime
- processed: boolean
- [other metrics columns]

#### dim_post_details
- post_key: string (primary key)
- [other details columns]

### Output Tables

#### fact_social_media_ai_metrics
- id: bytes (primary key)
- post_key: string
- date_key: datetime
- analysis_time: datetime
- sentiment: string
- engagement_score: integer
- content_category: string

#### dim_social_media_ai_details
- id: bytes (primary key)
- post_key: string
- last_update_time: datetime
- key_topics: json
- recommendations: json

## Usage

1. Deploy the Lambda function using AWS SAM or your preferred deployment method
2. Configure the necessary IAM roles and permissions
3. Set up the database tables
4. Create the required AWS Secrets Manager secret
5. The function will:
   - Fetch unprocessed records (limited to 100 at a time)
   - Process them through OpenAI
   - Store the enriched data
   - Mark the source records as processed

## Error Handling

The service includes comprehensive error handling:
- Database operation errors
- OpenAI API errors
- Data validation errors
- Configuration errors

All errors are logged with appropriate context for debugging.

## Security

- Uses AWS Secrets Manager for credential management
- Implements parameterized queries to prevent SQL injection
- Validates input data before processing
- Uses proper IAM roles and permissions

## Monitoring

Monitor the function using AWS CloudWatch:
- Execution time
- Error rates
- Records processed
- API latency

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request
