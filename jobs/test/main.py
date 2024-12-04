import asyncio
import json
import logging
import os

import yaml

from src.common import RootConfig
from src.connectors import Connector
from src.inference import Client

logger = logging.getLogger(__name__)


async def main(config: dict[str, type]) -> None:
    config = RootConfig.parse_obj(config)
    logger.debug(f"Config: {json.dumps(config.dict(), indent=2)}")

    extractor = Connector.get_instance(config.Extract)
    transformer = Client.get_instance(config.Transform)
    loader = Connector.get_instance(config.Load)

    # define model
    try:
        transformer.load()
    except ImportError as e:
        raise RuntimeError("Failed to load model") from e

    # read data
    try:
        data = extractor.read()
        logger.info(f"Read {len(data)} records from source")
    except Exception as e:
        raise RuntimeError("Failed to read data from source") from e

    # enrich
    try:
        results = transformer.enrich(data)
        logger.info(f"Enriched {len(results)}/{len(data)} records")
    except Exception as e:
        raise RuntimeError("Failed to enrich data") from e

    # write
    async with loader.client as dest:
        await dest.write(results)
        logger.info(f"Wrote {len(results)} results to destination")


if __name__ == "__main__":
    import time

    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    start = time.time()
    asyncio.run(main(config))
    end = time.time()

    logger = logging.getLogger(__name__)
    logger.info(f"Workflow completed in {end - start:.2f} seconds")
