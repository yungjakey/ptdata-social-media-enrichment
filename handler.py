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

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def get_secret() -> dict[str, str]:
    secret_name = os.environ["OPENAI_SECRET_NAME"]
    region_name = os.environ.get("AWS_REGION", "eu-central-1")

    session = aioboto3.Session()
    async with session.client(service_name="secretsmanager", region_name=region_name) as client:
        try:
            get_secret_value_response = await client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            logger.error(f"Failed to retrieve secret: {e}")
            raise e
        else:
            if "SecretString" in get_secret_value_response:
                secret = json.loads(get_secret_value_response["SecretString"])
                return secret
            else:
                logger.error("Secret value is not a string")
                raise ValueError("Secret value must be a string")


def load_config(event: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, str]]:
    """Parse input from API Gateway."""
    # Parse input
    path = event.get("path", "")
    if (model_type := path.strip("/")) is None:
        raise ValueError("Model type not specified in path")

    # Get model type from path
    config_path = os.path.join(os.path.dirname(__file__), "config", f"{model_type}.yaml")
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Config file not found for model type: {model_type}")
        raise

    # Get query parameters from query string
    kwargs = {}
    query_params = event.get("queryStringParameters", {})

    if (max_records := query_params.get("max_records")) is not None:
        try:
            kwargs["max_records"] = int(max_records)
        except Exception as e:
            logger.error(f"Invalid max_records parameter, using default: {e}")

    if (drop := query_params.get("drop")) is not None:
        try:
            if drop.lower() == "true":
                kwargs["drop"] = True
        except Exception as e:
            logger.error(f"Invalid drop parameter, using default: {e}")

    return model_type, config, kwargs


def lambda_handler(event: dict[str, Any]) -> dict[str, Any]:
    """AWS Lambda handler for all model types."""
    try:
        # Get OpenAI credentials from Secrets Manager
        secrets = asyncio.run(get_secret())
        os.environ["OPENAI_API_KEY"] = secrets["OPENAI_API_KEY"]
        os.environ["OPENAI_API_BASE"] = secrets["OPENAI_API_BASE"]

        # Parse input
        model_type, config, kwargs = load_config(event)

        # Load config and run main
        start = time.time()
        n = asyncio.run(main(config, **kwargs))
        end = time.time()

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
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid configuration"}),
        }
    except Exception as e:
        logger.error(f"Error processing request: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }
