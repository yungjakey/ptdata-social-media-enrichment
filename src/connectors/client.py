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
        self.cutoff_time = datetime.now() - timedelta(hours=self.config.source.time_filter_hours)

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
            catalog = await self.catalog
            table = catalog.load_table((self.config.target.database, self.config.target.table))
        except NoSuchTableError:
            logger.info("Target table does not exist yet")
            return {}

        # Only look at records within our time window
        logger.debug(f"Cutoff time: {self.cutoff_time.isoformat()}")
        df = table.scan(
            row_filter=f"{self.config.target.datetime_field} >= '{self.cutoff_time.isoformat()}'",
        ).to_arrow()

        if len(df) == 0:
            logger.debug("No records found in time window")
            return {}

        # Create mapping of index -> processed_at
        logger.debug(f"Target table schema: {df.schema}")
        index_col = df[self.config.target.index_field]
        datetime_col = df[self.config.target.datetime_field]

        # Log some sample timestamps for debugging
        sample_size = min(5, len(datetime_col))
        sample_times = [str(dt) for dt in datetime_col[:sample_size]]
        logger.debug(f"Sample timestamps from target: {sample_times}")

        processed = {
            idx: datetime.fromisoformat(str(dt))
            for idx, dt in zip(index_col.to_pylist(), datetime_col.to_pylist(), strict=False)
        }
        logger.debug(f"Found {len(processed)} previously processed records in time window")

        return processed

    async def _get_source_table(self, source) -> pa.Table | None:
        """Get records from a single source table."""
        try:
            # Load table using (database, table) tuple
            catalog = await self.catalog
            table = catalog.load_table((source.database, source.table))

            # Filter by datetime field using scan API
            df = table.scan(
                row_filter=f"{source.datetime_field} >= '{self.cutoff_time.isoformat()}'",
            ).to_arrow()
            logger.info(
                f"Records after time filter in {source.table}: {len(df)} (cutoff: {self.cutoff_time.isoformat()})"
            )
            return df
        except Exception as e:
            logger.error(f"Error reading source table {source.table}: {e}")
            return None

    async def _get_source_records(self) -> list[pa.Table]:
        """Get records from source tables concurrently."""
        tasks = [self._get_source_table(source) for source in self.config.source.tables]
        results = await asyncio.gather(*tasks)
        tables = [table for table in results if table is not None]
        return tables

    async def drop_target_table(self) -> None:
        """Drop the target table if it exists."""
        try:
            catalog = await self.catalog
            catalog.drop_table((self.config.target.database, self.config.target.table))
            logger.info(f"Dropped table {self.config.target.database}.{self.config.target.table}")
        except NoSuchTableError:
            logger.info("Target table does not exist, nothing to drop")

    async def read(self, drop: bool = False) -> pa.Table:
        """Read records from source tables."""
        if drop:
            await self.drop_target_table()

        # Get records from source tables
        tables = await self._get_source_records()
        if not tables:
            return pa.Table.from_pylist([])

        # Get mapping of index keys to last processing time in target table
        source_dt = await self._get_processed_records()

        # Filter each source table before joining
        filtered_tables = []
        for table, source_config in zip(tables, self.config.source.tables, strict=False):
            if source_dt:
                index_col = table[source_config.index_field]
                datetime_col = table[source_config.datetime_field]

                # Convert to Python for easier comparison
                indices = index_col.to_pylist()
                datetimes = [datetime.fromisoformat(str(dt)) for dt in datetime_col.to_pylist()]

                # Only keep records where source datetime > target datetime
                keep_indices = []
                for idx, target_dt in zip(indices, datetimes, strict=False):
                    if idx not in source_dt or target_dt > source_dt[idx]:
                        keep_indices.append(True)
                    else:
                        keep_indices.append(False)

                # Apply filter
                table = table.filter(pa.array(keep_indices))

            filtered_tables.append(table)

        # Join filtered tables
        result = filtered_tables[0]
        for table in filtered_tables[1:]:
            result = result.join(table, keys=self.config.target.index_field)

        logger.info(f"Returning {self.config.source.max_records}/{len(result)}")
        return result.slice(0, self.config.source.max_records)

    async def write(
        self,
        records: pa.Table,
        model: type[BaseModel],
        now: datetime | None = None,
    ) -> None:
        """Write records to target table."""

        if not now:
            now = datetime.now()

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
