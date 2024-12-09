#!/usr/bin/env python3

import asyncio
import json
import logging
import os
from datetime import datetime

from src.inference import InferenceClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


async def test_enrichment():
    """Test enrichment with mock data."""
    base_path = os.path.dirname(__file__)
    mock_records = json.loads(open(os.path.join(base_path, "samples/source.json")).read())

    config = {
        "api_key": os.getenv("OPENAI_API_KEY"),
        "api_base": os.getenv("OPENAI_API_BASE"),
        "response_format": "sentiment",
        "temperature": 0.0,  # Make responses deterministic
    }

    async with InferenceClient.from_config(config) as provider:
        results = await provider.process_batch(mock_records)
        logger.info(f"Processed {len(results)} records")
        for result in results:
            logger.info(f"Result: {json.dumps(result, indent=2, default=json_serial)}")


if __name__ == "__main__":
    asyncio.run(test_enrichment())
