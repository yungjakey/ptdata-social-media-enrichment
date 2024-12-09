import asyncio
import json
import logging
import os
import time

import yaml

from src.common import RootConfig
from src.connectors import AWSConnector
from src.inference import InferenceClient

logger = logging.getLogger(__name__)


async def main(config: RootConfig) -> None:
    """Run the main process."""
    logger.info("Starting process")

    # validate configs
    logger.debug(f"Validating config: {config.model_dump_json(indent=2)}")

    # read
    async with (
        AWSConnector.from_config(config.connector) as connector,
        InferenceClient.from_config(config.inference) as provider,
    ):
        try:
            records = await connector.read()
            logger.info(f"Read {len(records)} records from source")

            if not records:
                logger.info("No records found to process, exiting")
                return

            try:
                results = await provider.process_batch(records)
                await connector.write(
                    records=results,
                    response_format=config.inference.response_format,
                )
            except Exception as e:
                logger.error(f"Error processing records: {e}", exc_info=True)
                raise

        except Exception as e:
            logger.error(f"Error reading data: {e}", exc_info=True)
            raise

    logger.info("Process completed successfully")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    logger.info(f"Loading config from {config_path}")

    with open(config_path) as f:
        config = yaml.safe_load(f)
        logger.debug(f"Raw config: {json.dumps(config, indent=2)}")

    config = RootConfig.model_validate(config)
    logger.debug(f"Validated config: {config.model_dump_json(indent=2)}")

    start = time.time()
    asyncio.run(main(config))
    end = time.time()

    logger.info(f"Workflow completed in {end - start:.2f} seconds")
