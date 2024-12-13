import asyncio
import logging
import random
from datetime import datetime, timedelta

import aioboto3
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

from src.common.component import ComponentFactory
from src.common.utils import IcebergConverter

from .config import AWSConnectorConfig

logger = logging.getLogger(__name__)


class AWSConnector(ComponentFactory[AWSConnectorConfig]):
    """AWS connector implementation."""

    _config_type = AWSConnectorConfig

    def __init__(self, config: AWSConnectorConfig) -> None:
        """Initialize AWS connector."""
        super().__init__(config)
        self.session = aioboto3.Session(region_name=self.config.region)

        credentials = self.session.get_credentials()
        if not credentials:
            raise Exception("No credentials found")

        frozen_credentials = credentials.get_frozen_credentials()

        try:
            self.catalog = load_catalog(
                type="glue",
                uri=f"glue://{self.config.region}",
                s3_file_io_impl="pyiceberg.io.s3.S3FileIO",
                **{
                    "s3.access-key-id": frozen_credentials.access_key,
                    "s3.secret-access-key": frozen_credentials.secret_key,
                    "s3.session-token": frozen_credentials.token,
                    "s3.connect-timeout": 120,
                },
            )
        except Exception as e:
            raise Exception(f"Error loading catalog: {e}") from e

        self.cutoff_time = datetime.now()
        if self.config.source.time_filter_hours:
            self.cutoff_time = datetime.now() - timedelta(
                hours=self.config.source.time_filter_hours
            )

    async def _get_processed_records(self) -> dict[str, datetime]:
        """Get mapping of index keys to their last processing time."""
        try:
            table = self.catalog.load_table((self.config.target.database, self.config.target.table))
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
        source = self.config.source.tables[idx]
        try:
            table = self.catalog.load_table((source.database, source.table))

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

    async def read(self, max_records: int = 100, drop: bool = False) -> pa.Table:
        """Read records from source tables."""

        if drop:
            await self.drop_target_table()

        # Get records from source tables
        tables = await asyncio.gather(
            *[self._get_source_table(j) for j in range(len(self.config.source.tables))],
            return_exceptions=True,
        )

        # Get mapping of index keys to last processing time in target table
        processed = await self._get_processed_records()

        # Filter each source table before joining
        if not processed or len(processed) == 0:
            filtered_tables = [t for t in tables if t and not isinstance(t, BaseException)]
        else:
            filtered_tables = []
            for table, config in zip(tables, self.config.source.tables, strict=False):
                if not table or isinstance(table, BaseException):
                    logger.error(f"Error reading {config.table}: {table}")
                    return pa.Table.from_pylist([])

                mask = [
                    idx not in processed or dt > processed[idx]
                    for idx, dt in zip(
                        table.column(config.index_field).to_pylist(),
                        table.column(config.datetime_field).to_pylist(),
                        strict=False,
                    )
                ]
                filtered_tables.append(table.filter(pa.array(mask)))

        # Join filtered tables
        df = filtered_tables[0]
        for table in filtered_tables[1:]:
            df = df.join(table, keys=self.config.target.index_field)

        logger.info(f"Returning {max_records}/{len(df)}")

        # add metadata
        metadata = {
            "index": self.config.target.index_field,
        }
        df = df.replace_schema_metadata(metadata)

        # get random selection of records
        indices = random.sample(range(df.num_rows), k=max_records)
        return df.take(indices)

    async def write(
        self,
        records: pa.Table,
        now: datetime | None = None,
    ) -> None:
        """Write records to target table."""

        if not now:
            now = datetime.now()

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
            table = self.catalog.load_table(table_identifier)
        except NoSuchTableError:
            logger.debug(f"Creating schema from {records.schema} and {datetime_field.name}")
            schema = IcebergConverter.to_iceberg_schema(records.schema, datetime_field)

            kwargs = {
                "identifier": table_identifier,
                "schema": schema,
                "location": self.config.target.location,
            }
            if self.config.target.partition_by:
                kwargs["partition_spec"] = IcebergConverter.to_partition_spec(
                    self.config.target.partition_by
                )

            table = self.catalog.create_table(**kwargs)

        records = records.append_column(
            datetime_field,
            pa.array([now] * len(records)),
        )

        # Write records to table
        logger.info(f"Writing {records.schema}")
        table.append(records)

    async def drop_target_table(self) -> None:
        """Drop the target table if it exists."""
        try:
            self.catalog.drop_table((self.config.target.database, self.config.target.table))
            logger.info(f"Dropped table {self.config.target.database}.{self.config.target.table}")
        except NoSuchTableError:
            logger.info("Target table does not exist, nothing to drop")
