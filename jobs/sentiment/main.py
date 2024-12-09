import asyncio
import logging
import os
import signal
import time

import yaml

from src.common import RootConfig
from src.connectors import AWSConnector
from src.inference import InferenceClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Function to handle termination signals
def signal_handler(sig, frame):
    logger.info("Signal received, exiting gracefully...")
    exit(0)


async def main(config: RootConfig) -> None:
    """Run the main process."""
    logger.info("Starting process")

    # validate configs
    logger.debug(f"Validating config: {config.model_dump_json(indent=2)}")

    async with (
        AWSConnector.from_config(config.connector) as connector,
        InferenceClient.from_config(config.inference) as provider,
    ):
        logger.info("Reading data from source")
        records = await connector.read()
        logger.info(f"Read {len(records)} records from source")

        # check
        if not records:
            logger.info("No records found to process, exiting")
            return

        # process
        logger.info("Processing records")
        results = await provider.process_batch(records)
        logger.info(f"Processed {len(results)}/{len(records)} records")

        if not results:
            logger.warning("No valid records to process, exiting")
            return

        # write
        await connector.write(
            records=results,
            model=provider.model,
        )
        logger.info(f"Wrote {len(results)} records to destination")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    logger.info(f"Loading config from {config_path}")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    config = RootConfig.model_validate(config)
    logger.debug(f"Validated config: {config.model_dump_json(indent=2)}")

    start = time.time()
    asyncio.run(main(config))
    end = time.time()

    logger.info(f"Workflow completed in {end - start:.2f} seconds")
