import asyncio
import logging
import random
from datetime import datetime, timedelta

import aioboto3
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
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
        self._session = None
        self._catalog = None
        self.cutoff_time = datetime.now()
        if self.config.source.time_filter_hours:
            self.cutoff_time = datetime.now() - timedelta(
                hours=self.config.source.time_filter_hours
            )

    @property
    def session(self) -> aioboto3.Session:
        if not self._session:
            self._session = aioboto3.Session(region_name=self.config.region)
        return self._session

    @property
    async def catalog(self) -> Catalog | None:
        credentials = await self.session.get_credentials()
        frozen_credentials = await credentials.get_frozen_credentials()

        try:
            self._catalog = load_catalog(
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

        return self._catalog

    async def drop_target_table(self, catalog: Catalog) -> None:
        """Drop the target table if it exists."""

        try:
            catalog.drop_table((self.config.target.database, self.config.target.table))
            logger.info(f"Dropped table {self.config.target.database}.{self.config.target.table}")
        except NoSuchTableError:
            logger.info("Target table does not exist, nothing to drop")

    async def _get_processed_records(self, catalog: Catalog) -> dict[str, datetime]:
        """Get mapping of index keys to their last processing time."""

        try:
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

    async def _get_source_table(self, idx: int, catalog: Catalog) -> pa.Table | None:
        """Get records from a single source table."""
        source = self.config.source.tables[idx]

        try:
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

    async def read(self, max_records: int = 100, drop: bool = False) -> pa.Table:
        """Read records from source tables."""

        catalog = await self.catalog
        if not catalog:
            logger.error("Error getting catalog")
            return pa.Table.from_pylist([])

        if drop:
            await self.drop_target_table(catalog)

        # Get records from source tables
        tables = await asyncio.gather(
            *[self._get_source_table(j, catalog) for j in range(len(self.config.source.tables))],
            return_exceptions=True,
        )

        # Get mapping of index keys to last processing time in target table
        processed = await self._get_processed_records(catalog)

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

        if not filtered_tables or len(filtered_tables) == 0:
            logger.warning("No valid records found")
            return pa.Table.from_pylist([])

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
        indices = random.sample(range(df.num_rows), k=min(max_records, len(df)))
        if len(indices) == 0:
            logger.warning("No valid records found")
            return pa.Table.from_pylist([])

        return df.take(indices)

    async def write(
        self,
        records: pa.Table,
        now: datetime | None = None,
    ) -> None:
        """Write records to target table."""

        catalog = await self.catalog
        if not catalog:
            logger.error("Error getting catalog")
            return None

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
            table = catalog.load_table(table_identifier)
        except NoSuchTableError:
            logger.info(
                f"Creating schema from datetime field {datetime_field.name}: {records.schema.to_string()}"
            )
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

            table = catalog.create_table(**kwargs)

        records = records.append_column(
            datetime_field,
            pa.array([now] * len(records)),
        )

        # Write records to table
        logger.info(f"Writing {records.schema}")
        table.append(records)
