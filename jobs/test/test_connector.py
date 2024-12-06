#!/usr/bin/env python3

import asyncio
import json
import logging
import os

from pydantic import BaseModel, Field

from src.connectors import AWSConnector

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class MockRecord(BaseModel):
    """Mock social media post."""

    post_key: str = Field(..., description="Post key")
    post_message: str = Field(..., description="Post message")
    likes: str = Field(..., description="Number of likes")
    comments: str = Field(..., description="Number of comments")
    shares: str = Field(..., description="Number of shares")


async def test_connection():
    """Test AWS connector with mock data."""
    # Load configuration

    base_path = os.path.dirname(__file__)
    mock_records = json.loads(open(os.path.join(base_path, "samples/records.json")).read())

    config = {
        "bucket_name": "orf-tmp",
        "source_query": "SELECT * FROM dev_gold.fact_social_media_reaction_post",
    }

    async with AWSConnector.from_config(config) as connector:
        # Test reading
        # try:
        #     data = await connector.read()
        #     logger.info(f"Read {len(data)} records from source")
        #     for record in data:
        #         logger.info(f"Record: {json.dumps(record, indent=2)}")
        # except Exception as e:
        #     logger.error(f"Failed to read data from source: {e}")
        #     return

        # Test writing
        try:
            await connector.write(mock_records, MockRecord)
            logger.info(f"Wrote {len(mock_records)} records to destination")
        except Exception as e:
            logger.error(f"Failed to write data: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(test_connection())
