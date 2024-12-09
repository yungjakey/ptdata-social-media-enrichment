#!/usr/bin/env python

from pyiceberg.catalog import load_catalog

from src.connectors.utils import IcebergConverter
from src.inference.models.sentiment import Sentiment

# Test configuration
REGION = "eu-central-1"
DATABASE = "dev_test"
WAREHOUSE = "s3://orf-tmp/dev_test"
TABLE_NAME = "sentiment_test"
DATETIME_FIELD = "timestamp"
PARTITION_TRANSFORMS = ["year", "month", "day"]  # Could be ["year", "month"] or ["year"] etc.


def main():
    """Test Iceberg catalog configuration."""
    print("Testing Iceberg Catalog Configuration")

    # Create converter and get schema and partition spec
    converter = IcebergConverter(
        model=Sentiment,
        datetime_field=DATETIME_FIELD,
        partition_transforms=PARTITION_TRANSFORMS,
    )
    schema = converter.to_iceberg_schema()
    partition_spec = converter.to_partition_spec(schema)

    print(f"Schema: {schema}")
    print(f"Partition spec: {partition_spec}")

    # Load catalog and create table
    catalog = load_catalog(
        type="glue",
        uri=f"glue://{REGION}/{DATABASE}",
        warehouse=WAREHOUSE,
    )

    try:
        table = catalog.create_table(
            identifier=(DATABASE, TABLE_NAME),
            schema=schema,
            partition_spec=partition_spec,
            location=f"{WAREHOUSE}/{TABLE_NAME}",
        )
        print("Table created successfully")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("Loading existing table...")
            table = catalog.load_table((DATABASE, TABLE_NAME))
        else:
            raise

    print("Table details:")
    print(f"  Schema: {table.schema()}")
    print(f"  Location: {table.location()}")
    print(f"  Partitioning: {table.spec()}")


if __name__ == "__main__":
    main()
