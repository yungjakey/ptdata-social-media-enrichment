import asyncio
import logging
import random
from datetime import datetime, timedelta

import aioboto3
import pyarrow as pa
from pydantic import BaseModel
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError

from src.common.component import ComponentFactory
from src.common.utils import IcebergConverter

from .config import AWSConnectorConfig

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
        df = (
            table.scan(
                row_filter=f"{self.config.target.datetime_field} >= '{self.cutoff_time.isoformat()}'",
            )
            .to_arrow()
            .sort_by([(self.config.target.datetime_field, "descending")])
        )

        if len(df) == 0:
            logger.debug("No records found in time window")
            return {}

        # Create mapping of index -> processed_at
        try:
            processed = dict(
                zip(
                    df[self.config.target.index_field].to_pylist(),
                    df[self.config.target.datetime_field].to_pylist(),
                    strict=False,
                )
            )
            logger.info(
                f"Found {len(processed)} previously processed records in {self.config.target.table} after {self.cutoff_time.isoformat()}"
            )

            return processed

        except Exception as e:
            logger.error(
                f"Error getting last processed records from {self.config.target.table}: {e}"
            )
            return {}

    async def _get_source_table(self, idx: int) -> pa.Table | None:
        """Get records from a single source table."""
        try:
            source = self.config.source.tables[idx]
            catalog = await self.catalog
            table = catalog.load_table((source.database, source.table))

            df = (
                table.scan(
                    row_filter=f"{source.datetime_field} >= '{self.cutoff_time.isoformat()}'",
                )
                .to_arrow()
                .sort_by([(source.datetime_field, "descending")])
            )

            if len(df) == 0:
                logger.debug(f"No records found in {source.table}")
                return None

            logger.info(f"Read {len(df)} records from {source.table}")
            return df
        except Exception as e:
            logger.error(f"Error reading {source.table}: {e}")
            return None

    async def read(self, drop: bool = False) -> pa.Table:
        """Read records from source tables."""
        if drop:
            await self.drop_target_table()

        # Get records from source tables
        tables = await asyncio.gather(
            *[self._get_source_table(j) for j in range(len(self.config.source.tables))]
        )

        if not all(tables):
            for t, c in zip(tables, self.config.source.tables, strict=False):
                if t is None:
                    logger.warn(f"Source table {c.database}.{c.table} not found")

            return pa.table([])

        # Get mapping of index keys to last processing time in target table
        processed = await self._get_processed_records()

        # Filter each source table before joining
        if processed and len(processed) > 0:
            filtered_tables = []
            for table, config in zip(tables, self.config.source.tables, strict=False):
                mask = [
                    idx not in processed or dt > processed[idx]
                    for idx, dt in zip(
                        table.column(config.index_field).to_pylist(),
                        table.column(config.datetime_field).to_pylist(),
                        strict=False,
                    )
                ]
                filtered_tables.append(table.filter(pa.array(mask)))
        else:
            filtered_tables = tables

        # Join filtered tables
        df = filtered_tables[0]
        for table in filtered_tables[1:]:
            df = df.join(table, keys=self.config.target.index_field)

        logger.info(f"Returning {self.config.source.max_records}/{len(df)}")

        # add metadata
        metadata = {
            "index": self.config.target.index_field,
        }
        df = df.replace_schema_metadata(metadata)

        # get random selection of records
        indices = random.sample(range(table.num_rows), k=self.config.source.max_records)
        return df.take(indices)

    async def write(
        self,
        records: pa.Table,
        model: type[BaseModel],
        now: datetime | None = None,
    ) -> None:
        """Write records to target table."""

        if not now:
            now = datetime.now()

        catalog = await self.catalog
        table_identifier = (
            self.config.target.database,
            self.config.target.table,
        )
        datetime_field = pa.field(
            self.config.target.datetime_field,
            type=pa.timestamp("us"),
            nullable=False,
        )
        try:
            table = catalog.load_table(table_identifier)
        except NoSuchTableError:
            schema = IcebergConverter.to_iceberg_schema(records.schema, datetime_field)
            spec = IcebergConverter.to_partition_spec(self.config.target.partition_by)

            table = catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=spec,
                location=self.config.target.location,
            )

        records = records.append_column(
            datetime_field,
            pa.array([now] * len(records)),
        )

        # Write records to table
        table.append(records)

    async def drop_target_table(self) -> None:
        """Drop the target table if it exists."""
        try:
            catalog = await self.catalog
            catalog.drop_table((self.config.target.database, self.config.target.table))
            logger.info(f"Dropped table {self.config.target.database}.{self.config.target.table}")
        except NoSuchTableError:
            logger.info("Target table does not exist, nothing to drop")
