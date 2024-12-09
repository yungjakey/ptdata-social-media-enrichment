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

DEBUG = False
if DEBUG_VAL := os.getenv("DEBUG"):
    from ast import literal_eval

    try:
        DEBUG = literal_eval(DEBUG_VAL)
    except ValueError:
        logger.warning(f"Invalid DEBUG value {DEBUG_VAL}, defaulting to {DEBUG}")

if DEBUG:
    import json
    from pathlib import Path

    from src.common import DateTimeEncoder

    SOURCE = str(Path(__file__).parent.parent.parent / "samples" / "source.json")
    TARGET = str(Path(__file__).parent.parent.parent / "samples" / "target.json")

    def read_debug_data(path: str) -> list[dict[str, any]]:
        with open(path) as f:
            return json.load(f)

    def write_debug_data(recs: list[dict[str, any]], path: str) -> None:
        s = json.dumps(recs, indent=2, cls=DateTimeEncoder)
        logger.debug(f"Writing {s}")
        with open(path, "w") as f:
            f.write(s)

    logger.info(f"Running in DEBUG mode with local source {SOURCE} and target {TARGET}")


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
        # read
        if DEBUG and os.path.exists(SOURCE):
            logger.debug("Reading data from local")
            records = read_debug_data(SOURCE)
        else:
            logger.info("Reading data from source")
            records = await connector.read()
            logger.info(f"Read {len(records)} records from source")

        # check
        if not records:
            logger.info("No records found to process, exiting")
            return
        if DEBUG:
            logger.debug("Writing records to local")
            write_debug_data(records, SOURCE)

        # process
        if DEBUG and os.path.exists(TARGET):
            logger.debug("Reading from local")
            results = read_debug_data(TARGET)
        else:
            logger.info("Processing records")
            results = await provider.process_batch(records)
            logger.info(f"Processed {len(results)}/{len(records)} records")

        if not results:
            logger.warning("No valid records to process, exiting")
            return
        if DEBUG:
            logger.debug("Writing results to local")
            write_debug_data(results, TARGET)

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
