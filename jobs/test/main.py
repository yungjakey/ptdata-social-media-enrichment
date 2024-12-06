import asyncio
import json
import logging
import os

import yaml

from src.common import RootConfig
from src.connectors import AWSConnector
from src.inference import InferenceClient

logger = logging.getLogger(__name__)


async def main(config: dict[str, type]) -> None:
    try:
        config = RootConfig.model_validate(config)
    except ValueError as e:
        raise RuntimeError(f"Failed to parse config: {json.dumps(config, indent=2)}") from e

    async with (
        AWSConnector.from_config(config.connector) as connector,
        InferenceClient.from_config(config.inference) as provider,
    ):
        # read data
        try:
            data = await connector.read()
            logger.info(f"Read {len(data)} records from source")
        except Exception as e:
            raise RuntimeError("Failed to read data from source") from e

        if not data:
            raise RuntimeError("No data returned")

        # enrich
        try:
            results = await provider.process_batch(data)
            logger.info(f"Enriched {len(results)}/{len(data)} records")
        except Exception as e:
            raise RuntimeError("Failed to enrich data") from e

        if not results:
            raise RuntimeError("No results returned")

        # write
        try:
            await connector.write(results, provider.model)
            logger.info(f"Wrote {len(results)} results to destination")
        except Exception as e:
            raise RuntimeError("Failed to write results") from e


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
