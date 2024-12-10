import asyncio
import logging
from datetime import datetime, timedelta

import aioboto3
import pyarrow as pa
from pydantic import BaseModel
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError

from src.common.component import ComponentFactory

from .config import AWSConnectorConfig
from .utils import IcebergConverter

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class AWSConnector(ComponentFactory):
    """AWS connector implementation."""

    _config = AWSConnectorConfig

    def __init__(self, config: AWSConnectorConfig) -> None:
        """Initialize AWS connector."""
        super().__init__(config)
        self._session = None
        self._catalog = None

    @property
    def session(self) -> aioboto3.Session:
        if self._session is None:
            # In Lambda, this will use the environment credentials
            # Locally, this will use the default credential chain (including SSO)
            self._session = aioboto3.Session(region_name=self.config.region)
        return self._session

    @property
    async def catalog(self) -> Catalog:
        """Get the catalog instance."""
        if self._catalog is None:
            # Get credentials from boto3 session
            credentials = await self.session.get_credentials()
            frozen_credentials = await credentials.get_frozen_credentials()
            self._catalog = load_catalog(
                type="glue",
                uri=f"glue://{self.config.region}",
                s3_file_io_impl="pyiceberg.io.s3.S3FileIO",
                **{
                    "s3.access-key-id": frozen_credentials.access_key,
                    "s3.secret-access-key": frozen_credentials.secret_key,
                    "s3.session-token": frozen_credentials.token,
                },
            )
        return self._catalog

    async def _get_processed_records(self) -> dict[str, datetime]:
        """Get mapping of index keys to their last processing time."""
        try:
            # Use fact catalog for target table since it's a fact table
            catalog = await self.catalog
            table = catalog.load_table((self.config.target.database, self.config.target.table))

            df = table.scan().to_arrow()

            # Create mapping of index -> processed_at
            index_col = df[self.config.target.index_field]
            datetime_col = df[self.config.target.datetime_field]
            return {
                idx: datetime.fromisoformat(str(dt))
                for idx, dt in zip(index_col.to_pylist(), datetime_col.to_pylist(), strict=False)
            }
        except NoSuchTableError:
            logger.info("Target table does not exist yet")
            return {}

    async def _get_source_table(self, source) -> pa.Table | None:
        """Get records from a single source table."""
        try:
            # Load table using (database, table) tuple
            catalog = await self.catalog
            table = catalog.load_table((source.database, source.table))

            # Filter by datetime field using scan API
            df = table.scan(
                row_filter=f"{source.datetime_field} >= '{self.cutoff_time.isoformat()}'"
            ).to_arrow()

            return df
        except Exception as e:
            logger.error(f"Error reading from table {source.database}.{source.table}: {e}")
            return None

    async def _get_source_records(self) -> list[pa.Table]:
        """Get records from source tables concurrently."""
        tables = []
        self.cutoff_time = datetime.now() - timedelta(hours=self.config.source.time_filter_hours)

        # Create tasks for all source tables
        tasks = [self._get_source_table(source) for source in self.config.source.tables]

        # Wait for all tables to be read
        results = await asyncio.gather(*tasks)

        # Filter out None results from failed reads
        tables = [table for table in results if table is not None]

        return tables

    async def read(self) -> pa.Table:
        """Read records from source tables."""
        processed = await self._get_processed_records()
        tables = await self._get_source_records()

        if not len(tables):
            return pa.Table.from_pylist([])

        # Start with first table and join others sequentially
        result = tables[0]
        for table in tables[1:]:
            result = result.join(table, keys=self.config.target.index_field)

        # Filter out already processed records if any
        logger.debug(f"Number of records before filtering: {len(result)}")
        if processed:
            index_col = result[self.config.source.tables[0].index_field]
            mask = pa.compute.is_in(index_col, value_set=pa.array(list(processed.keys())))
            result = result.filter(pa.compute.invert(mask))
        logger.debug(f"Number of records after filtering: {len(result)}")

        return result

    async def write(self, records: pa.Table, model: type[BaseModel], now: datetime) -> None:
        """Write records to target table."""

        # Add processing timestamp
        timestamp_field = pa.field(
            self.config.target.datetime_field, pa.timestamp("us"), nullable=False
        )
        records = records.append_column(
            timestamp_field,
            pa.array([now] * len(records), type=pa.timestamp("us")),
        )

        try:
            catalog = await self.catalog
            table = catalog.load_table((self.config.target.database, self.config.target.table))
        except NoSuchTableError:
            # Get the field type from the records schema
            index_field = records.schema.field(self.config.target.index_field)
            additional_fields = {
                self.config.target.index_field: index_field.type,
            }
            converter = IcebergConverter(
                model=model,
                datetime_field=self.config.target.datetime_field,
                partition_transforms=self.config.target.partition_by,
                additional_fields=additional_fields,
            )
            schema = converter.to_iceberg_schema()
            spec = converter.to_partition_spec(schema)

            catalog = await self.catalog
            table = catalog.create_table(
                identifier=(self.config.target.database, self.config.target.table),
                schema=schema,
                partition_spec=spec,
                location=self.config.target.location,
            )

        # Write records to table
        table.append(records)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
