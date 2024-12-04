import asyncio
import logging
import os
import time

import yaml
from pydantic import BaseModel, create_model

from src.connectors import Connectors
from src.inference import InferenceClient


async def create_input_model(sample_data: dict) -> type[BaseModel]:
    model_fields = {}
    for key, value in sample_data.items():
        field_type = type(value)
        model_fields[key] = (field_type, ...)

    return create_model("DynamicInputModel", **model_fields)


async def main(config: dict[str, type]) -> None:
    logger = logging.getLogger(__name__)

    try:
        # Create connectors
        ic, oc = Connectors.from_config(config)

        # Initialize inference client
        inference_client = InferenceClient.get_instance(ic.config.model_dump())

        # Use async context managers to handle resources
        async with ic as source, oc as dest:
            # Read source data
            data = [record async for record in source.read()]
            logger.info(f"Read {len(data)} records from source")

            # Create input model dynamically from first record
            if not data:
                logger.warning("No input data found")
                return

            # Convert first record to dictionary for model creation
            first_record_dict = data[0].model_dump()
            InputModel = await create_input_model(first_record_dict)

            # Create output model from configuration
            OutputModel = create_model(
                "DynamicOutputModel",
                **{k: (eval(v), ...) for k, v in config["Model"]["schema"].items()},
            )

            # Process data through OpenAI
            results = await inference_client.process_batch(
                records=[r.model_dump() for r in data],
                input_model=InputModel,
                output_model=OutputModel,
            )
            logger.info(f"Processed {len(results)} records through OpenAI")

            # Write results to destination
            await dest.write(results)
            logger.info("Wrote results to destination")

    except Exception as e:
        logger.error(f"Workflow failed: {e}")
        raise


if __name__ == "__main__":
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
