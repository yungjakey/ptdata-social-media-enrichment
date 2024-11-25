"""Main entry point for social media enrichment pipeline."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any

from omegaconf import DictConfig, OmegaConf

from src.aws import AWSClient
from src.azure import OpenAIClient
from src.common import RootConfig
from src.models import Builder

logger = logging.getLogger(__name__)


def setup_logging(config: DictConfig) -> None:
    """Configure logging from config."""
    logging.basicConfig(
        level=config.level,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def init_config(config_path: str) -> RootConfig:
    """Initialize root configuration from YAML file."""
    config = OmegaConf.load(config_path)
    return RootConfig.from_config(config)


def create_model(tables: list[Table], prompts: dict[str, str]) -> tuple[type[Any], type[Any]]:
    """Create input and output models from Glue schema."""
    # Input model from Glue schema
    input_builder = Builder("Input")
    if "system" in prompts:
        input_builder.with_prompt(prompts["system"])
    for table in tables:
        schema = table.get_schema()
        input_builder.from_schema(schema)
    input_model = input_builder.build()

    # Output model from Glue schema
    output_builder = Builder("Output")
    for table in tables:
        schema = table.get_schema()
        output_builder.from_schema(schema)
    output_model = output_builder.build()

    return input_model, output_model


async def fetch_data(aws_client: AWSClient, config: RootConfig) -> list[dict[str, Any]]:
    """Fetch data from Athena and join on primary keys."""
    tables = []
    for source in config.aws.db.sources:
        table = aws_client.get_table(source.database, source.table)
        tables.append(table)

    # Build query to join tables on primary keys
    query = f"""
    SELECT *
    FROM {tables[0].database}.{tables[0].name} t0
    """
    for i, table in enumerate(tables[1:], 1):
        keys = " AND ".join(f"t0.{k} = t{i}.{k}" for k in config.aws.primary)
        query += f"""
        JOIN {table.database}.{table.name} t{i}
        ON {keys}
        """

    df = aws_client.execute_query(query, tables[0].database)
    return df.to_dict("records")


async def process_data(
    records: list[dict[str, Any]],
    openai_client: OpenAIClient,
    input_model: type[Any],
    prompts: dict[str, str],
) -> list[dict[str, Any]]:
    """Process data with OpenAI."""
    # Create prompts for each record
    batch_prompts = []
    for record in records:
        instance = input_model(**record)
        prompt = prompts["user"].format(json.dumps(instance.dict(), indent=2))
        batch_prompts.append({
            "model": "gpt-4",
            "messages": [
                {"role": "system", "content": prompts["system"]},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.3,
            "max_tokens": 1000,
        })

    # Process batch with OpenAI
    return await openai_client.process_batch(batch_prompts)


async def write_data(
    aws_client: AWSClient,
    config: RootConfig,
    results: list[dict[str, Any]],
    output_model: type[Any],
) -> None:
    """Write data back to AWS."""
    # Validate results with output model
    validated = [output_model(**result) for result in results]
    data = [model.dict() for model in validated]

    # Write to each target table
    for target in config.aws.db.targets:
        table = aws_client.get_table(target.database, target.table)
        columns = ", ".join(table.get_schema().keys())
        values = ", ".join(["%s"] * len(table.get_schema()))
        
        query = f"""
        INSERT INTO {target.database}.{target.table} ({columns})
        VALUES ({values})
        """
        
        aws_client.execute_query(query, target.database)


async def main_async() -> None:
    """Async main function."""
    try:
        # Initialize config
        config_path = Path(__file__).parent / "config.yaml"
        config = init_config(str(config_path))
        setup_logging(config.logging)

        # Initialize clients
        aws_client = AWSClient(output=config.aws.output)
        openai_client = OpenAIClient(
            api_key=config.openai.client.api_key,
            api_base=config.openai.client.base_url,
            max_workers=config.openai.max_workers,
        )

        async with openai_client:
            # Create models from schema
            tables = [
                aws_client.get_table(source.database, source.table)
                for source in config.aws.db.sources
            ]
            input_model, output_model = create_model(tables, config.openai.prompts)

            # Process pipeline
            records = await fetch_data(aws_client, config)
            results = await process_data(records, openai_client, input_model, config.openai.prompts)
            await write_data(aws_client, config, results, output_model)

    except Exception as e:
        logger.error("Pipeline failed: %s", str(e))
        raise


def main() -> None:
    """Main entry point."""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error("Pipeline failed: %s", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
