"""AWS client for Glue and Athena services."""

from __future__ import annotations

import logging
import time

from boto3 import Session
from botocore.exceptions import ClientError
from pandas import DataFrame

from src.aws.types import AWSConfig, State

logger = logging.getLogger(__name__)


class AWSClient:
    """A client for interacting with AWS Glue and Athena services.

    This client provides functionality to:
    - Execute SQL queries on AWS Athena
    - Retrieve table metadata from AWS Glue
    - Handle query pagination and result processing
    - Manage AWS session and client lifecycle

    The client implements both sync and async context managers for resource cleanup.

    Attributes:
        config (AWSConfig): Configuration for AWS services including region and output location
        athena: Boto3 Athena client for query execution
        glue: Boto3 Glue client for metadata operations
    """

    _GLUE2PY = {
        "string": str,
        "int": int,
        "bigint": int,
        "double": float,
        "float": float,
        "boolean": bool,
        "binary": bytes,
        "timestamp": str,
        "date": str,
        "map": dict,
        "struct": dict,
    }

    def __init__(self, config: AWSConfig) -> None:
        """Initialize AWS client with config."""
        self.config = config
        self._session = Session(region_name=str(self.config.region.value))

        try:
            self.athena = self._session.client("athena")
            self.glue = self._session.client("glue")
            logger.debug("AWS client initialized with config: %s", self.config)
        except Exception as e:
            self.__exit__(type(e), e, e.__traceback__)
            raise Exception("Failed to initialize AWS clients") from e

    def get_table(
        self,
        database: str,
        table_name: str,
    ) -> dict[str, type]:
        """Get table details from Glue catalog."""
        try:
            logger.debug("Getting table: %s.%s", database, table_name)
            response = self.glue.get_table(DatabaseName=database, Name=table_name)
            table = response["Table"]

            columns = table.get("StorageDescriptor", {}).get("Columns", [])
            schema = {col["Name"]: self._GLUE2PY.get(col["Type"].lower(), str) for col in columns}

            return schema
        except ClientError as e:
            err = e.response.get("Error", {})
            c = err.get("Code", "")
            m = err.get("Message", "")

            raise Exception(f"Failed to get table {database}.{table_name}: {c} - {m}") from e

    def submit_query(
        self,
        query: str,
        database: str,
        output_location: str | None = None,
        params: dict | None = None,
    ) -> str | None:
        """Execute Athena query and return results as DataFrame."""
        output_location = output_location or self.config.output

        if params:
            # BAD
            query = query.format(**params)

        try:
            response = self.athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": database},
                ResultConfiguration={"OutputLocation": output_location},
            )
            qid = response.get("QueryExecutionId")
            logger.debug("Query execution ID: %s", qid)

            return qid

        except ClientError as e:
            err = e.response.get("Error", {})
            c = err.get("Code", "")
            m = err.get("Message", "")
            raise Exception("AWS error executing query: %s - %s", c, m) from e

    def get_results(self, query_execution_id: str) -> DataFrame:
        """Retrieve query results as DataFrame."""
        try:
            logger.debug("Fetching results for query: %s", query_execution_id)
            response = self.athena.get_query_results(QueryExecutionId=query_execution_id)
            result_set = response.get("ResultSet", {})

            if not result_set:
                raise ValueError("No ResultSet returned from Athena")

            # Process results into rows
            rows = []
            for row in result_set.get("Rows", []):
                data = row.get("Data", [])
                values = [field.get("VarCharValue", "") for field in data]
                rows.append(values)

            # First row contains column names
            if not rows:
                return DataFrame()  # Return empty DataFrame if no rows

            columns = rows[0]  # First row has column names
            data = rows[1:]  # Remaining rows are data

            df = DataFrame(data, columns=columns)
            logger.info("Query completed successfully with %d rows", len(df))

            return df

        except ClientError as e:
            err = e.response.get("Error", {})
            c = err.get("Code", "")
            m = err.get("Message", "")
            raise Exception(f"Error fetching Athena results: {c} - {m}") from e

    def run(
        self,
        query: str,
        database: str,
        output_location: str | None = None,
        params: dict | None = None,
    ):
        """Execute query and return results as DataFrame."""
        query_id = self.submit_query(query, database, output_location, params)
        if not query_id:
            raise RuntimeError("Failed to get query execution ID")

        logger.debug("Waiting for query completion: %s", query_id)
        for attempt in range(self.config.max_retries + 1):
            response = self.athena.get_query_execution(QueryExecutionId=query_id)
            status = response["QueryExecution"]["Status"]
            state = State(status["State"])

            if state.ok:
                logger.info("Query completed successfully")
                results = self.athena.get_query_results(QueryExecutionId=query_id)
                return results
            elif state.nok:
                reason = status.get("StateChangeReason", "Unknown reason")
                logger.error("Query failed: %s", reason)
                raise RuntimeError(f"Query failed: {reason}")
            if not state.pending:
                logger.warning("Query state not recognized: %s", status.get("State", ""))
            if state.pending and attempt < self.config.max_retries:
                wait = 2**attempt * self.config.wait_time
                logger.debug("Waiting for %d seconds before next attempt", wait)
                time.sleep(wait)

        # Always return results after exhausting retries
        raise RuntimeError("Max retries exceeded")

    def __enter__(self) -> AWSClient:
        """Context manager entry."""
        logger.debug("Entering AWS client context")
        return self

    def __exit__(self, exc_type: type | None, exc_val: type, exc_tb: type) -> None:
        """Context manager exit."""
        logger.debug("Exiting AWS client context")
        try:
            if hasattr(self, "athena"):
                self.athena.close()
            if hasattr(self, "glue"):
                self.glue.close()
            # Session doesn't need to be closed
        except Exception as e:
            logger.error("Error closing AWS clients: %s", str(e))
