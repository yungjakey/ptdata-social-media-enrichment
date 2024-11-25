# Social Media Analytics Enrichment

This project enriches social media data using AWS Glue/Athena for data extraction and OpenAI for content analysis.

## Architecture

### Data Flow
1. **Input Data Sources**
   - Social media metrics from `fact_social_media_reaction_post` table
   - Post details from `dim_post_details` table

2. **Processing Pipeline**
   ```
   [RDS Tables] → [AWS Glue/Athena] → [OpenAI API] → [Enriched Data Tables]
   ```

3. **Output Data**
   - AI-enriched metrics in `fact_social_media_ai_metrics`
   - AI-enriched details in `dim_social_media_ai_details`

## Project Structure

```
src/
├── aws/              # AWS service clients
│   └── client.py     # Glue and Athena operations
├── azure/            # Azure/OpenAI integration
│   └── client.py     # OpenAI batch processing
├── common/           # Shared utilities
│   ├── config.py     # Configuration classes
│   └── utils.py      # Helper functions
├── models/           # Data models
│   ├── builder.py    # Dynamic schema builder
│   ├── input.py      # Input data models
│   └── output.py     # Output data models
├── config.yaml       # Configuration file
└── example.py        # Example implementation
```

## Configuration

The project uses a `config.yaml` file with the following structure:

```yaml
aws:
  output: "s3://bucket/path/"    # S3 output path
  primary: ["id"]                # Primary key for joins
  db:
    sources:                     # Source tables
      - database: "dev_gold"
        table: "fact_social_media_reaction_post"
      - database: "dev_gold"
        table: "dim_post_details"
    targets:                     # Target tables
      - database: "dev_gold"
        table: "dim_social_media_ai_details"
      - database: "dev_gold"
        table: "fact_social_media_ai_metrics"

openai:
  api_version: "2024-10-01-preview"
  engine: "gpt-4o-mini"
  temperature: 0.3
  max_tokens: 1000
  workers: 5
```

## Prerequisites

1. AWS credentials configured with access to:
   - AWS Glue
   - Amazon Athena
   - Amazon S3

2. OpenAI API key set as environment variable:
   ```bash
   export OPENAI_API_KEY="your-api-key"
   ```

## Usage

The example implementation in `example.py` demonstrates:

1. Schema Creation:
   - Extracts schemas from AWS Glue metadata
   - Creates dynamic Pydantic models for validation

2. Data Processing:
   - Queries source tables from Athena
   - Joins data using configured primary keys
   - Validates data against schemas

3. OpenAI Integration:
   - Processes data in batches
   - Uses configurable workers for parallelization
   - Handles rate limiting and errors

To run the example:

```bash
python -m src.example
```

## Development

The codebase uses:
- Pydantic for data validation
- AWS Boto3 for AWS services
- OpenAI's API for content analysis
- Asyncio for concurrent processing

Key components:

1. `Builder`: Creates dynamic Pydantic models from Glue metadata
2. `AWSClient`: Handles Glue/Athena operations
3. `OpenAIClient`: Manages batch processing with OpenAI

## Error Handling

The code includes comprehensive error handling for:
- AWS service errors
- Schema validation errors
- OpenAI API errors
- Batch processing failures

Errors are logged using Python's logging module with configurable levels.

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
