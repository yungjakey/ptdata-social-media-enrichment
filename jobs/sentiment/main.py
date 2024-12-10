"""Sentiment analysis job."""

import asyncio
import logging
import os
import signal

import yaml

from src.common import RootConfig
from src.connectors import AWSConnector
from src.inference import InferenceClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def signal_handler(sig, frame):
    """Handle termination signals."""
    logger.info("Signal received, exiting gracefully...")
    exit(0)


async def main(config: RootConfig, drop: bool = False) -> None:
    """Run the main process."""
    logger.info("Starting process")
    logger.debug(f"Validating config: {config.model_dump_json(indent=2)}")

    connector = AWSConnector.from_config(config.connector)
    index_field = connector.config.target.index_field

    # init early to check config
    async with InferenceClient.from_config(config.inference) as provider:
        try:
            # Read and process records
            logger.info("Reading data from source")
            records = await connector.read(drop=drop)
            logger.info(f"Read {len(records)} records from source")
            logger.debug(f"Source schema: {records.schema}")

            if not records:
                logger.info("No records found to process, exiting")
                return

            # Process records
            logger.info("Processing records")
            results = await provider.process_batch(records=records, index=index_field)
            logger.info(f"Processed {len(results)}/{len(records)} records")
            logger.debug(f"Result schema: {results.schema}")

            # join index from records to results
            records = records.select([connector.config.target.index_field])
            results = records.join(results, keys=connector.config.target.index_field)
            if not results:
                logger.warning("No valid records to process, exiting")
                return

            # Write results with current UTC timestamp
            await connector.write(records=results, model=provider.model)
            logger.info(f"Wrote {len(results)} records to destination")

        except Exception as e:
            logger.error(f"Error processing records: {e}", exc_info=True)
            raise


if __name__ == "__main__":
    import time

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    try:
        # Register signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Load and validate configuration
        config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        logger.info(f"Loading config from {config_path}")

        with open(config_path) as f:
            config = yaml.safe_load(f)

        config = RootConfig.model_validate(config)
        logger.debug(f"Validated config: {config.model_dump_json(indent=2)}")

        # Run job and measure time
        start = time.time()
        asyncio.run(main(config, drop=False))  # Drop table for testing
        end = time.time()

        logger.info(f"Workflow completed in {end - start:.2f} seconds")

    except Exception as e:
        logger.error(f"Error running job: {e}", exc_info=True)
        raise
