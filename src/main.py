"""Main entry point for social media analytics."""

import asyncio
import json
import logging
import os
from typing import Any

import boto3
from botocore.exceptions import ClientError
from omegaconf import OmegaConf

from src.processor import BatchProcessor
from src.utils import (
    fetch_records,
    load_schema_from_database,
    write_records,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_aws_session():
    """
    Get an AWS session using SSO credentials.
    """
    try:
        # Use the sandbox profile
        session = boto3.Session(profile_name="sandbox", region_name="eu-central-1")

        # Test the session credentials
        sts = session.client("sts")
        identity = sts.get_caller_identity()
        print(f"Using AWS credentials for: {identity['Arn']}")

        return session
    except ClientError as e:
        logger.error(f"Failed to get AWS session: {e}")
        raise


def get_secret(secret_arn: str, session: boto3.Session) -> dict[str, Any]:
    """Get OpenAI credentials from AWS Secrets Manager."""
    client = session.client("secretsmanager")
    try:
        response = client.get_secret_value(SecretId=secret_arn)
        return json.loads(response["SecretString"])
    except ClientError as e:
        logger.error(f"Failed to get secret: {e}")
        raise


async def async_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Process social media analytics asynchronously."""
    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        config = OmegaConf.load(config_path)

        # Get AWS session and create Athena client
        session = get_aws_session()
        athena_client = session.client("athena")

        # Get OpenAI credentials
        openai_creds = get_secret(config.database.secret_arn, session)

        # Load schema from database
        schema = await load_schema_from_database(athena_client, config.database)

        # Initialize batch processor
        processor = BatchProcessor(
            api_key=openai_creds["OPENAI_API_KEY"],
            api_base=openai_creds["OPENAI_API_BASE"],
            config=config.openai,
            schema=schema,
        )

        # Fetch records
        records = await fetch_records(athena_client, config.database)
        if not records:
            logger.info("No records to process")
            return {"statusCode": 200, "body": "No records to process"}

        # Process records in batches
        results = await processor.process_records(records, config.prompts)

        # Extract successful evaluations
        processed_facts = []
        processed_dims = []
        processed_ids = []

        for evaluation in results.successful_evaluations:
            processed_facts.append(evaluation.metrics.to_dict())
            processed_dims.append(evaluation.details.to_dict())
            processed_ids.append(evaluation.metrics.id)

        if processed_ids:
            # Write processed records
            if processed_facts:
                await write_records(athena_client, "facts", processed_facts, config.database)

            if processed_dims:
                await write_records(athena_client, "dims", processed_dims, config.database)

            # # Mark records as processed
            # await mark_records_as_processed(
            #     athena_client,
            #     processed_ids,
            #     config.database
            # )

        return {
            "statusCode": 200,
            "body": f"Processed {len(processed_ids)} records successfully, {len(results.failed_post_keys)} failed",
        }

    except Exception as e:
        logger.error(f"Handler failed: {e}")
        return {"statusCode": 500, "body": str(e)}


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """AWS Lambda handler entry point."""
    return asyncio.run(async_handler(event, context))


if __name__ == "__main__":
    # For local testing
    asyncio.run(async_handler({}, None))
