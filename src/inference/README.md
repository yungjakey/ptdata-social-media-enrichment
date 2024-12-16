# Inference Module

This module handles Azure OpenAI integration for batch processing social media content, with a focus on sentiment analysis.

## Core Components

### `InferenceClient` (`client.py`)
- Asynchronous Azure OpenAI client for batch processing
- Manages concurrent record processing with semaphore-based rate limiting
- Implements exponential backoff retry for rate limits
- Handles JSON response parsing and validation
- Supports PyArrow table input/output

### `InferenceConfig` (`config.py`)
- Azure OpenAI configuration (endpoint, deployment, version)
- Batch processing settings (workers, timeouts)
- Model parameters (temperature, max_tokens)
- Response format configuration

### Models (`models/`)
- Pydantic models for structured AI responses
- System prompts for each model type
- Type validation and conversion


## Features

### Batch Processing
- Process PyArrow tables in batches
- Concurrent record processing with configurable workers
- Preserves index field types from source data
- Handles errors per record without failing the batch

### Rate Limiting & Retries
```python
@tenacity.retry(
    retry=tenacity.retry_if_exception_type(RateLimitError),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=30),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
```

### Resource Management
- Async context manager support
- Proper client cleanup
- Task tracking and completion
- Semaphore-based concurrency control

## Configuration

```yaml
inference:
  # Azure OpenAI settings
  version: "2024-08-01-preview"
  deployment: "gpt-4"
  engine: "gpt-4"
  
  # Batch processing
  workers: 5
  exclude_fields: ["field_to_exclude"]
  
  # Model parameters
  temperature: 0.7
  max_tokens: 1000
```

## Usage Example

```python
from src.inference.models.sentiment import Sentiment

async with InferenceClient(config) as client:
    # Process batch of records with sentiment analysis
    result_table = await client.process_batch(
        records=input_table,
        index="id_field"
    )
```

### Record Processing
```python
# Each record is processed with the model's system prompt
sysmsg = self.model.get_prompt()
usrmsg = json.dumps(filtered_record, indent=2, cls=CustomEncoder)

messages = [
    {"role": "system", "content": sysmsg},
    {"role": "user", "content": usrmsg},
]

# Response is validated against the Pydantic model
completion = await self.client.beta.chat.completions.parse(
    messages=messages,
    response_format=self.model,
    model=self.config.engine,
    temperature=self.config.temperature,
    max_tokens=self.config.max_tokens,
)
```

## Best Practices

1. **Resource Management**
   - Use async context manager
   - Ensure proper cleanup
   - Monitor active tasks

2. **Configuration**
   - Set appropriate worker count
   - Configure retry parameters
   - Adjust model parameters

3. **Error Handling**
   - Monitor per-record errors
   - Handle rate limits
   - Validate responses

4. **Performance**
   - Tune worker count
   - Monitor batch sizes
   - Track processing times

5. **Model Development**
   - Define clear prompts
   - Use strict type validation
   - Set appropriate ranges for numeric fields
