"""Sentiment analysis job."""

import asyncio
import logging
import os

import yaml

from src.common import RootConfig
from src.connectors import AWSConnector
from src.inference import InferenceClient

logger = logging.getLogger(__name__)


def signal_handler(sig, frame):
    """Handle termination signals."""
    logger.info("Signal received, exiting gracefully...")
    exit(0)


async def main(config: dict[str, type], drop: bool = False) -> int:
    """Run the main process."""
    logger.info("Starting process")

    # Validate config
    config = RootConfig.model_validate(config)
    logger.debug(f"Validated config: {config.model_dump_json(indent=2)}")

    # Init connector
    connector = AWSConnector.from_config(config.connector)

    # Read and process records
    logger.info("Reading data from source")
    records = await connector.read(drop=drop)
    logger.info(f"Read {len(records)} records from source")
    logger.debug(f"Source schema: {records.schema}")

    # check
    if not records or not records.num_rows:
        logger.info("No records found to process, exiting")
        return 0

    # Process records
    async with InferenceClient.from_config(config.inference) as provider:
        logger.info("Processing records")
        results = await provider.process_batch(records=records)
        logger.info(f"Processed {len(results)}/{len(records)} records")
        logger.debug(f"Result schema: {results.schema}")

    # check
    if not results or not results.num_rows:
        logger.warning("No valid records to process, exiting")
        return 0

    # Write results with current UTC timestamp
    await connector.write(records=results)
    logger.info(f"Wrote {len(results)} records to destination")


if __name__ == "__main__":
    import argparse
    import signal
    import time

    # Configure logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # read model from args
    parser = argparse.ArgumentParser(description="Run sentiment analysis job")
    parser.add_argument("--model", type=str, default="sentiment", help="Model to use")
    args = parser.parse_args()

    # Load and validate configuration
    config_path = os.path.join("config", f"{args.model}.yaml")
    logger.info(f"Loading config from {config_path}")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Run job and measure time
    start = time.time()
    asyncio.run(main(config, drop=True))  # Drop table for testing
    end = time.time()

    logger.info(f"Workflow completed in {end - start:.2f} seconds")
