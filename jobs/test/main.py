"""AWS/Azure workflow implementation."""

import asyncio
import logging
import os
from typing import Any

import yaml
from botocore.exceptions import ClientError
from pydantic import BaseModel

from src.connectors.types import Connector
from src.inference.client import OpenAIClient
from src.models.types import ModelBuilder

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def process_batch(
    records: list[dict[str, Any]],
    model: BaseModel,
    provider: OpenAIClient,
) -> list[dict[str, Any]]:
    """Process a batch of records through the model and provider."""
    tasks = []
    for record in records:
        tasks.append(
            provider.generate(
                model=model,
                input_data=record,
            )
        )

    results = []
    try:
        for result, record in zip(
            await asyncio.gather(*tasks, return_exceptions=True),
            records,
            strict=False,
        ):
            if isinstance(result, Exception):
                logger.error(f"Failed to process record: {result}")
                continue
            results.append({**record, **result})
    except Exception as e:
        logger.error(f"Batch processing error: {e}")
        raise

    return results


async def main():
    """Run the AWS/Azure workflow."""
    # Load configuration
    with open(os.path.join(os.path.dirname(__file__), "config.yaml")) as f:
        config = yaml.safe_load(f)

    # Setup components from config
    connector = Connector.from_config(**config["connector"])
    model = ModelBuilder(config["model"]["name"]).from_schema(config["model"]["schema"]).build()
    provider = OpenAIClient(config["provider"])

    try:
        # Read and collect all records
        records = []
        async for record in connector.read():
            records.append(record)

        # Process through Azure OpenAI
        enriched_records = await process_batch(
            records=records,
            model=model,
            provider=provider,
        )

        # Write enriched records back
        await connector.write(enriched_records)

    except ClientError as e:
        logger.error(f"AWS operation failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Workflow failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
