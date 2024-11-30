"""AWS/Azure workflow implementation."""

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from typing import Any

import yaml
from dotenv import load_dotenv

from src.connectors.builder import ConnectorBuilder
from src.models.builder import ModelBuilder
from src.providers.builder import ProviderBuilder

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def process_records(
    records: AsyncIterator[dict[str, Any]],
    model: Any,
    provider: Any,
) -> AsyncIterator[dict[str, Any]]:
    """Process records through model and provider."""
    async for record in records:
        # Process through model/provider
        result = await provider.generate(
            model=model,
            input_data=record,
        )

        # Merge with input record
        yield {
            **record,
            **result,
        }


async def main():
    """Run the AWS/Azure workflow."""
    # Load environment variables
    load_dotenv()

    # Load configuration
    with open(os.path.join(os.path.dirname(__file__), "config.yaml")) as f:
        config = yaml.safe_load(f)

    # Setup components from config
    connector = ConnectorBuilder.from_config(config["connector"])
    model = ModelBuilder.from_config(config["model"])
    provider = ProviderBuilder.from_config(config["provider"])

    try:
        # Read records
        source_records = await connector.read()

        # Process through Azure OpenAI
        enriched_records = process_records(
            records=source_records,
            model=model,
            provider=provider,
        )

        # Write enriched records back
        await connector.write(enriched_records)

    except Exception as e:
        logger.error(f"Workflow failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
