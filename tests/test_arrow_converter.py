import json
import logging
from datetime import datetime

import pyarrow as pa
import pytest
from pyiceberg.exceptions import NoSuchTableError

from src.common.utils import ArrowConverter, CustomEncoder
from src.connectors import AWSConnector
from src.inference.models.user_needs import UserNeeds

logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
TEST_DATE = "2024-12-18T12:04:55.773695"
NOW = datetime.strptime(TEST_DATE, "%Y-%m-%dT%H:%M:%S.%f")


@pytest.mark.asyncio
async def test_arrow_to_iceberg_workflow():
    # Load configuration from user_needs.yaml
    config = {
        "source": {
            "tables": [
                {
                    "database": "prod_gold",
                    "table": "dim_post_details",
                    "datetime_field": "last_update_time",
                    "index_field": "post_key",
                }
            ],
            "time_filter_hours": 6,
        },
        "target": {
            "database": "dev_test",
            "table": "schema_test",
            "datetime_field": "written_at",
            "index_field": "post_key",
            "location": "s3://aws-orf-social-media-analytics/dev/test/ai",
            "partition_by": "month",
        },
    }

    index_col = config["target"]["index_field"]

    # Create mock data as a dictionary
    mock_data = {
        "wissen": {
            "need": "keep me engaged",
            "score": 0.9,
            "confidence": 0.95,
        },
        "verstehen": {
            "need": "educate me",
            "score": 0.8,
            "confidence": 0.9,
        },
        "fuehlen": {
            "need": "connect me",
            "score": 0.85,
            "confidence": 0.92,
        },
        "machen": {
            "need": "inspire me",
            "score": 0.88,
            "confidence": 0.93,
        },
        "summary": "This is a summary of the analysis",
    }

    # Create the resulting schema
    schema = ArrowConverter.to_arrow_schema(UserNeeds)

    # Convert the mock UserNeeds data to an Arrow Table using the schema
    arrow_table = pa.Table.from_pylist([mock_data], schema=schema)

    # Append the index column to the Arrow table
    input_data = arrow_table.append_column(index_col, pa.array([bytes(1)]))  # Example index values

    # Get connector
    connector = AWSConnector.from_config(config)
    catalog = await connector.catalog

    # Get catalog
    try:
        await connector.drop_target_table(catalog)
    except NoSuchTableError:
        pass

    # Write the mock data to the Iceberg table using the connector
    await connector.write(input_data, now=NOW)

    # Retrieve the written data from the Iceberg table using the catalog
    output_data = (
        catalog.load_table((config["target"]["database"], config["target"]["table"]))
        .scan(row_filter=f"{config['target']['datetime_field']} >= '{TEST_DATE}'")
        .to_arrow()
        .to_pylist()
    )

    # log
    logging.info("Input data:")
    for row in input_data.to_pylist():
        logging.info(json.dumps(row, indent=2, cls=CustomEncoder))

    logging.info("Output data:")
    for row in output_data:
        logging.info(json.dumps(row, indent=2, cls=CustomEncoder))
