# PTData Social Media Enrichment

A modular AWS Lambda-based system for enriching social media data using Azure OpenAI's language models, with Apache Iceberg for data storage and PyArrow for efficient data processing.

## TODO!

- [ ] Fix timeout
- [ ] Document new job structure
- [ ] Github workflow for redeploying on change

## Architecture Overview

### Core Components

1. **Connectors Module** ([`src/connectors/`](src/connectors/README.md))
   - Asynchronous AWS integration using aioboto3
   - Apache Iceberg table management via pyiceberg
   - PyArrow-based data processing
   - Concurrent source table reading
   - Incremental processing support

2. **Inference Module** ([`src/inference/`](src/inference/README.md))
   - Azure OpenAI integration for batch processing
   - Semaphore-based rate limiting with exponential backoff
   - PyArrow table input/output
   - Pydantic models for response validation

3. **Common Module** ([`src/common/`](src/common/README.md))
   - Component factory pattern implementation
   - Pydantic-based configuration system
   - Type-safe configuration validation
   - Shared utilities and type definitions

4. **Lambda Jobs** ([`jobs/`](jobs/README.md))
   - Generic Lambda handler with dynamic job loading
   - Job-specific configurations and implementations
   - Sentiment analysis processing pipeline
   - AWS Secrets Manager integration

### Key Features

- **Modular Design**: Factory pattern for extensible components (see [`src/common/`](src/common/README.md))
- **Type Safety**: Pydantic models throughout the stack
- **Data Processing**: PyArrow and Iceberg integration (see [`src/connectors/`](src/connectors/README.md))
- **AI Integration**: Azure OpenAI with rate limiting (see [`src/inference/`](src/inference/README.md))
- **Job Framework**: Lambda-based processing (see TODO)

### System Overview

```mermaid
graph LR
    Ingestion["Ingestion<br/>Lambda"] -->|"API Gateway<br/>HTTP Trigger"| Lambda["Sentiment<br/>Lambda"]
    Source[("Source Data<br/>Iceberg Tables")] --> Lambda
    Lambda --> Target[("Target Data<br/>Iceberg Tables")]
    
    Lambda <--> |"Sentiment<br/>Analysis"| OpenAI["Azure OpenAI"]

```

### Detailed Data Flow

```mermaid
graph LR
    subgraph Input
        Source[("Source Table")] --> Reader[("PyArrow<br/>Reader")]
    end

    subgraph Processing
        Batch["Batch<br/>Processor"] --> API["OpenAI<br/>API"]
    end

    subgraph Output
        Writer[("PyArrow<br/>Writer")] --> Target[("Target<br/>Table")]
    end

    Reader --> Batch
    API --> Writer
```

### Optimizations

#### Performance
- Concurrent data loading and processing with asyncio
- Memory-efficient PyArrow tables

#### Cost
- Iceberg metadata filtering
- Efficient batch sizes
- Lambda memory tuning
- PyArrow optimizations


## Configuration

The system uses a hierarchical configuration system:

### 1. Job Configuration (`jobs/sentiment/config.yaml`) 
TODO

### 2. AWS Configuration (`template.yaml`)
- Lambda function definitions
- IAM roles and permissions
- Resource allocations

### 3. Component Configurations
- Connector settings (see [`src/connectors/README.md`](src/connectors/README.md))
- Inference settings (see [`src/inference/README.md`](src/inference/README.md))
- Model configurations (see [`src/inference/models/`](src/inference/README.md#models))

## Setup and Installation

### Prerequisites

- Python 3.11+
- Poetry for dependency management
- AWS CLI and SAM CLI
- AWS account with appropriate permissions

### Installation

```bash
# Install dependencies
poetry install

# Configure AWS credentials
aws configure

# Set up environment variables
cp .env.example .env
# Edit .env with your settings
```

## Development

### Local Development

```bash
# Run tests
poetry run pytest

# Format and lint
poetry run ruff format .
poetry run ruff check .

# Type checking
poetry run mypy .
```

### Local Testing

```bash
# Test specific job
cd jobs/sentiment
python main.py

# Test with SAM
sam local invoke -e events/sentiment.json
```

## Deployment

### Using AWS SAM

```bash
# Build and deploy
sam build
sam deploy [--guided]
```

## Component Details

### Connectors ([`src/connectors/`](src/connectors/README.md))
- Iceberg table management
- PyArrow data processing
- Incremental processing
- Type conversion system

### Inference ([`src/inference/`](src/inference/README.md))
- Azure OpenAI integration
- Batch processing
- Rate limiting
- Response validation

### Common ([`src/common/`](src/common/README.md))
- Component factories
- Configuration system
- Type definitions
- Shared utilities
