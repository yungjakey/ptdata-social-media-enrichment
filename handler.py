import asyncio
import json
import logging
import os
import time
from typing import Any

import aioboto3
import yaml
from botocore.exceptions import ClientError
from pydantic import ValidationError

from main import main

# Setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def get_secret() -> dict[str, str]:
    """Fetch OpenAI credentials from AWS Secrets Manager."""
    logger.info("Fetching secrets from Secrets Manager...")
    secret_name = os.environ["OPENAI_SECRET_NAME"]
    region_name = os.environ.get("AWS_REGION", "eu-central-1")

    session = aioboto3.Session()
    async with session.client(service_name="secretsmanager", region_name=region_name) as client:
        try:
            response = await client.get_secret_value(SecretId=secret_name)
            if "SecretString" in response:
                logger.info("Secrets fetched successfully.")
                return json.loads(response["SecretString"])
            else:
                raise ValueError("Secret value is not a string")
        except ClientError as e:
            logger.error(f"Failed to retrieve secret: {e}")
            raise


async def load_config(event: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
    """Load model configuration and parameters from the event."""
    logger.info("Loading configuration from event...")
    path = event.get("path", "")
    if not path:
        raise ValueError("Model type not specified in path")

    model_type = path.strip("/")
    config_path = os.path.join(os.path.dirname(__file__), "config", f"{model_type}.yaml")

    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
            logger.info(f"Configuration for {model_type} loaded successfully.")
    except FileNotFoundError:
        logger.error(f"Config file not found for model type: {model_type}")
        raise

    kwargs = {}
    query_params = event.get("queryStringParameters", {})

    if (max_records := query_params.get("max_records")) is not None:
        try:
            kwargs["max_records"] = int(max_records)
        except ValueError:
            logger.warning("Invalid max_records parameter, using default")

    if (drop := query_params.get("drop")) is not None:
        kwargs["drop"] = drop.lower() == "true"

    return model_type, config, kwargs


async def process_request(event: dict[str, Any]) -> dict[str, Any]:
    """Process the incoming Lambda request."""
    logger.info(f"Received event: {json.dumps(event)}")

    # Fetch OpenAI credentials
    secrets = await get_secret()
    os.environ["OPENAI_API_KEY"] = secrets["OPENAI_API_KEY"]
    os.environ["OPENAI_API_BASE"] = secrets["OPENAI_API_BASE"]

    try:
        # Load model config and parameters
        model_type, config, kwargs = await load_config(event)

        # Execute main processing
        logger.info("Starting main processing...")
        start = time.time()
        n = await main(config, **kwargs)
        end = time.time()

        logger.info(f"Processing completed successfully in {end - start:.2f} seconds")
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": f"Successfully applied {model_type} to {n} records in {end - start:.2f} seconds"
                }
            ),
        }
    except ValidationError as e:
        logger.error(f"Config validation error: {e}")
        return {"statusCode": 400, "body": json.dumps({"error": "Invalid configuration"})}
    except Exception as e:
        logger.error(f"Error processing request: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


def lambda_handler(event: dict[str, Any], context: Any | None = None) -> dict[str, Any]:
    """AWS Lambda entry point."""
    logger.info("Starting Lambda handler...")
    try:
        return asyncio.run(process_request(event))
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": "Unhandled server error"})}
