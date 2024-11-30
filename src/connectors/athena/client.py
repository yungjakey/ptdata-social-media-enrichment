"""AWS client for Glue and Athena services."""

from __future__ import annotations

import logging
import time

import pandas as pd
from boto3 import Session
from botocore.exceptions import ClientError
from pandas import DataFrame

from src.aws.types import AWSConfig, State

logger = logging.getLogger(__name__)


class AWSClient:
    """AWS client for Glue and Athena services."""

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

    def _handle_client_error(
        self, e: ClientError, operation: str, context: str | None = None
    ) -> Exception:
        """Handle AWS client errors."""

        # Extract error details
        err = e.response.get("Error", {})
        code = err.get("Code", "Unknown")
        message = err.get("Message", "No details provided")

        # Construct detailed error message
        error_details = f"{operation} failed"
        if context:
            error_details += f" for {context}"
        error_details += f": {code} - {message}"

        logger.error(error_details)
        return Exception(error_details)

    def _submit_query(
        self,
        query: str,
        database: str,
        output_location: str | None = None,
    ) -> str:
        """Submit query to Athena."""

        try:
            # Use default output location if not provided
            output_location = output_location or self.config.output

            # Validate inputs
            if not query or not query.strip():
                raise ValueError("Query cannot be empty")

            if not database:
                raise ValueError("Database name is required")

            # Start query execution
            response = self.athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": database},
                ResultConfiguration={"OutputLocation": output_location},
            )

            # Extract and validate query execution ID
            query_id = response.get("QueryExecutionId")
            if not query_id:
                raise RuntimeError("Failed to obtain query execution ID")

            logger.debug("Submitted query to %s with execution ID: %s", database, query_id)
            return query_id

        except ClientError as e:
            raise self._handle_client_error(e, "Submitting Athena query", database) from e
        except (ValueError, RuntimeError) as e:
            raise Exception(f"Failed to submit query: {e}") from e

    def _parse_results(self, results: dict) -> DataFrame:
        """Parse Athena query results."""

        result_set = results.get("ResultSet", {})

        if not result_set:
            logger.warning("No ResultSet returned from Athena")
            return DataFrame()

        # Safely extract rows and header
        rows_data = result_set.get("Rows", [])
        if not rows_data:
            logger.warning("No rows found in ResultSet")
            return DataFrame()

        # Extract header (first row)
        header_row = rows_data[0].get("Data", [])
        columns = [col.get("VarCharValue", f"col_{i}") for (i, col) in enumerate(header_row)]

        # Extract data rows (skip header)
        data_rows = []
        for row in rows_data[1:]:
            row_data = row.get("Data", [])
            data_row = [
                field.get("VarCharValue", "")
                for field in row_data[: len(columns)]  # Ensure consistent column count
            ]
            data_rows.append(data_row)

        # Create DataFrame
        if not data_rows:
            logger.warning("No data rows found")
            return DataFrame(columns=columns)

        df = DataFrame(data_rows, columns=columns)

        # Robust type inference
        for col in df.columns:
            # Attempt to convert to numeric, preserving original if fails
            try:
                numeric_col = pd.to_numeric(df[col], errors="coerce")
                if not numeric_col.isna().all():
                    df[col] = numeric_col
            except TypeError:
                pass

        logger.info(f"Parsed results with {len(df)} rows and {len(df.columns)} columns")
        return df

    def get_table(
        self,
        database: str,
        table_name: str,
    ) -> dict[str, type]:
        """Get table metadata from Glue."""
        try:
            logger.debug("Fetching table metadata: %s.%s", database, table_name)

            response = self.glue.get_table(DatabaseName=database, Name=table_name)
            table = response["Table"]

            # Extract columns and map to Python types
            columns = table.get("StorageDescriptor", {}).get("Columns", [])
            schema = {col["Name"]: self._GLUE2PY.get(col["Type"].lower(), str) for col in columns}

            logger.info(f"Successfully retrieved schema for {database}.{table_name}")
            return schema

        except ClientError as e:
            raise self._handle_client_error(
                e, "Retrieving table metadata", f"{database}.{table_name}"
            ) from e

    def run(
        self,
        query: str,
        database: str,
        output_location: str | None = None,
    ) -> DataFrame:
        """Execute query and return results as DataFrame."""
        query_id = self._submit_query(query, database, output_location)

        for attempt in range(self.config.max_retries + 1):
            # Get query execution status
            response = self.athena.get_query_execution(QueryExecutionId=query_id)
            status = response.get("QueryExecution", {}).get("Status", {})
            state = State(status.get("State", ""))

            if state.ok:
                # Fetch and parse results
                results = self.athena.get_query_results(QueryExecutionId=query_id)
                return self._parse_results(results)

            if state.nok:
                reason = status.get("StateChangeReason", "Unknown reason")
                logger.error("Query failed: %s", reason)
                raise RuntimeError(f"Query failed: {reason}")

            if not state.pending:
                logger.warning("Query state not recognized: %s", status.get("State", ""))

            # Exponential backoff for pending queries
            if state.pending and attempt < self.config.max_retries:
                wait = 2**attempt * self.config.wait_time
                logger.debug("Waiting for %d seconds before next attempt", wait)
                time.sleep(wait)

        raise RuntimeError("Max retries exceeded for query execution")

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
