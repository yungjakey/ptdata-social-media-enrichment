#!/usr/bin/env python3

import asyncio
import logging
from datetime import datetime

import numpy as np
import pyarrow as pa

from src.connectors import AWSConnector
from src.inference.models.sentiment import Sentiment

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_mock_data(size: int = 10) -> pa.Table:
    """Create mock data for testing."""
    rng = np.random.default_rng(42)

    data = {
        "post_key": [f"post_{i}" for i in range(size)],
        "score": rng.random(size=size),
        "impact": rng.random(size=size),
        "sentiment": rng.choice(["positive", "negative", "neutral"], size=size),
        "confidence": rng.random(size=size),
    }

    mock_data = pa.Table.from_pydict(data)
    logging.info(f"Created mock data with schema:\n{mock_data.schema}")
    return mock_data


async def test_connector() -> None:
    """Test AWS connector."""
    config = {
        "warehouse": "s3://aws-orf-social-media-analytics/dev/test",
        "source": {
            "tables": [
                {
                    "database": "dev_test",
                    "table": "sentiment",
                    "datetime_field": "last_update_time",
                    "index_field": "post_key",
                    "location": "dev/test/fact/social_media_sentiment",
                }
            ],
            "time_filter_hours": 240,
            "max_records": 10,
        },
        "target": {
            "database": "dev_test",
            "table": "sentiment",
            "datetime_field": "last_update_time",
            "index_field": "post_key",
            "location": "s3://aws-orf-social-media-analytics/dev/test/fact/social_media_sentiment",
            "partition_by": ["year"],
        },
    }

    mock_data = create_mock_data()
    logging.info("Testing write to target table...")

    async with AWSConnector.from_config(config) as connector:
        try:
            await connector.write(records=mock_data, model=Sentiment, now=datetime.now())
            logging.info("Wrote mock results to target table")

            logging.info("Testing read from target table...")
            read_records = await connector.read()
            logging.info(f"Read {len(read_records)} records from target table")
        finally:
            # Cleanup: drop the table
            catalog = await connector.catalog
            if catalog.table_exists((config["target"]["database"], config["target"]["table"])):
                catalog.drop_table((config["target"]["database"], config["target"]["table"]))
                logging.info("Dropped target table")


if __name__ == "__main__":
    asyncio.run(test_connector())
