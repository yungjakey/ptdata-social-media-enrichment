#!/usr/bin/env python3

import asyncio
import json
import logging
import os

from pydantic import BaseModel, Field

from src.connectors import AWSConnector

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MockRecord(BaseModel):
    """Mock social media post."""

    sentiment: str = Field(..., description="Sentiment")
    score: float = Field(..., description="Sentiment score")
    impact: float = Field(..., description="Post impact")
    confidence: float = Field(..., description="Analysis confidence")


def load_mock_data() -> list[dict[str, str | int]]:
    """Load mock data from JSON file."""
    base_path = os.path.dirname(__file__)
    with open(os.path.join(base_path, "samples/target.json")) as f:
        return json.loads(f.read())


async def test_connector() -> None:
    """Test AWS connector with mock data."""
    mock_data = load_mock_data()
    database = "dev_test"
    table_name = "some_ai_source"
    bucket_name = "orf-tmp"

    config = {
        "bucket_name": bucket_name,
        "source_query": f"""
            SELECT * FROM {database}.{table_name}
            WHERE year = 2024 AND month = 12 AND day = 9
            LIMIT 100
        """,  # noqa: S608
        "table_config": {
            "database": database,
            "table_name": table_name,
            "partitions": [
                {"Name": "year", "Type": "int"},
                {"Name": "month", "Type": "int"},
                {"Name": "day", "Type": "int"},
            ],
        },
    }

    async with AWSConnector.from_config(config) as connector:
        # Drop table if it exists
        logger.info("Dropping table if it exists...")
        await connector._drop_table()

        # Test writing to S3 and Glue
        logger.info("Testing write to S3.")
        await connector.write(mock_data, MockRecord)
        logger.info(f"Wrote {len(mock_data)} records and updated Glue table")

        # List S3 files
        path = os.path.join(database, table_name)
        files = await connector.list(path)
        logger.info(f"Found {len(files)} files in {path}")

        for f in files[: min(3, len(files))]:
            logger.info(
                f"{f['Key']} ({f['Size']}b) - last modified {f['LastModified']:%Y-%m-%d %H:%M:%S}"
            )

        # Read from the created table
        logger.info("Testing read from created table...")
        results = await connector.read()
        logger.info(f"Read {len(results)} records from created table")

        for r in results:
            logger.info(json.dumps(r, indent=2))


if __name__ == "__main__":
    asyncio.run(test_connector())
