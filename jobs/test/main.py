"""AWS/Azure workflow implementation."""

import asyncio
import logging
import os

import yaml

from src.common import RootConfig

# from src.connectors import ConnectorProtocol
# from src.inference import OpenAIClient
# from src.models import ModelBuilder

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main(config: dict[str, type]):
    """Run the AWS/Azure workflow."""
    try:
        config = RootConfig.from_dict(**config)
    except Exception as e:
        import json

        raise ValueError(f"Invalid configuration: {json.dumps(config)}") from e

    # TODO: use async context managers to
    # 1.) read source data and create input model from it
    # 2.) create output model from config
    # 3.) send source data to OpenAI concurrently
    # 4.) write output data to destination


if __name__ == "__main__":
    import time

    # Load configuration
    with open(os.path.join(os.path.dirname(__file__), "config.yaml")) as f:
        config = yaml.safe_load(f)

    start = time.time()
    asyncio.run(main(config))
    end = time.time()

    logger.info(f"Workflow completed in {end - start:.2f} seconds")
