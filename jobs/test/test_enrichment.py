#!/usr/bin/env python3

import asyncio
import json
import logging
import os

from src.inference import InferenceClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mock data
mock_records = [
    {
        "post_key": "123",
        "post_message": "I absolutely love this product! Best purchase ever! 😍",
        "likes": "1000",
        "comments": "50",
        "shares": "25",
    },
    {
        "post_key": "456",
        "post_message": "This is terrible service. Never buying again. 😡",
        "likes": "10",
        "comments": "30",
        "shares": "5",
    },
]


async def test_enrichment():
    """Test enrichment with mock data."""
    if (key := os.getenv("OPENAI_API_KEY")) is None:
        logger.error("OPENAI_API_KEY environment variable not set")
        return

    if (base := os.getenv("OPENAI_API_BASE")) is None:
        logger.error("OPENAI_API_BASE environment variable not set")
        return

    config = {
        "api_base": base,
        "api_key": key,
        "response_format": "sentiment",
    }

    async with InferenceClient.from_config(config) as provider:
        try:
            results = await provider.process_batch(mock_records)
            logger.info(f"Processed {len(results)} records")
            for result in results:
                logger.info(f"Result: {json.dumps(result, indent=2)}")
        except Exception as e:
            logger.error(f"Failed to process batch: {e}")


if __name__ == "__main__":
    asyncio.run(test_enrichment())
