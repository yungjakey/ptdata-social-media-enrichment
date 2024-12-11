import asyncio
import json
import logging
import os
from typing import Any

import boto3
import yaml
from botocore.exceptions import ClientError
from pydantic import ValidationError

from main import main

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def load_config(model_type: str) -> dict:
    """Load config from yaml file."""
    config_path = os.path.join(os.path.dirname(__file__), "config", f"{model_type}.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)

    return config


def get_secret():
    secret_name = os.environ["OPENAI_SECRET_NAME"]
    region_name = os.environ.get("AWS_REGION", "eu-central-1")

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
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


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """AWS Lambda handler for all model types."""
    try:
        # Get OpenAI credentials from Secrets Manager
        secrets = get_secret()
        os.environ["OPENAI_API_KEY"] = secrets["OPENAI_API_KEY"]
        os.environ["OPENAI_API_BASE"] = secrets["OPENAI_API_BASE"]

        # Get model type from path
        path = event.get("path", "")
        model_type = path.strip("/")  # e.g., "sentiment" from "/sentiment"
        if not model_type:
            raise ValueError("Model type not specified in path")

        # Load config and run main
        config = load_config(model_type)
        n = asyncio.run(main(config, drop=False))

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Successfully applied {model_type} to {n} records"}),
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
