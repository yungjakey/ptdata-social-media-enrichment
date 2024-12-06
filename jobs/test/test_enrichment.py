#!/usr/bin/env python3

import asyncio
import json
import logging
import os

from src.inference import InferenceClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_enrichment():
    """Test enrichment with mock data."""
    base_path = os.path.dirname(__file__)
    mock_records = json.loads(open(os.path.join(base_path, "samples/records.json")).read())

    config = {
        "api_key": os.getenv("OPENAI_API_KEY"),
        "api_base": os.getenv("OPENAI_API_BASE"),
        "response_format": "sentiment",
    }

    async with InferenceClient.from_config(config) as provider:
        try:
            results = await provider.process_batch(mock_records)
            logger.info(f"Processed {len(results)} records")
        except Exception as e:
            logger.error(f"Failed to process batch: {e}")


if __name__ == "__main__":
    asyncio.run(test_enrichment())
