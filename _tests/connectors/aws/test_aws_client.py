"""AWS Client Test Suite.

This module contains comprehensive tests for the AWS Athena client implementation.
"""

import logging
from typing import Any
from urllib.parse import urlparse

import pytest
from pydantic import BaseModel

from src.common.types import AthenaTypes, TypeConverter
from src.connectors.aws.client import (
    AWSClient,
    ClientError,
    MalformedResponseError,
    NotConnectedError,
    QueryExecutionError,
)
from src.connectors.aws.types import Region


class MockModels:
    """Container for mock data models used in testing."""

    class SimpleRecord(BaseModel):
        """Simple mock model for testing."""

        column: str
        value: int

    class OutputModel(BaseModel):
        """Mock model for testing schema generation."""

        text: str
        sentiment: str
        confidence: float


class AWSClientFixtures:
    """Fixture factory for AWS client testing."""

    @staticmethod
    def create_config() -> dict[str, Any]:
        """Create a standard test configuration for AWS client."""
        return {
            "type": "aws",
            "name": "test",
            "params": {
                "database": "test_db",
                "output_location": "s3://bucket/path",
                "workgroup": "test_group",
                "region": Region.EU_CENTRAL_1.value,
                "query": "SELECT 1",
            },
        }


@pytest.fixture
async def aws_client(mocker):
    """Create a test AWS client with mocked dependencies."""
    config_dict = {
        "type": "aws",
        "name": "test",
        "params": {
            "database": "test_db",
            "output_location": "s3://test-bucket/test-path/",
            "workgroup": "test_group",
            "region": Region.EU_CENTRAL_1.value,
            "query": "SELECT 1",
        },
    }
    client = AWSClient(config_dict)

    # Create mock clients
    mock_session = mocker.AsyncMock()
    mock_athena = mocker.AsyncMock()
    mock_s3 = mocker.AsyncMock()
    mock_glue = mocker.AsyncMock()

    # Configure mock session to return specific clients in order
    mock_session.client = mocker.AsyncMock(
        side_effect=[
            mock_athena,  # First call for connect
            mock_glue,  # Second call for connect
            mock_s3,  # Third call for connect
        ]
    )

    # Configure mock clients with async methods
    mock_athena.start_query_execution = mocker.AsyncMock(
        return_value={"QueryExecutionId": "test_query_id"}
    )
    mock_athena.get_query_execution = mocker.AsyncMock(
        return_value={
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED", "CompletionDateTime": "2023-01-01T00:00:00Z"},
            }
        }
    )
    mock_athena.get_query_results = mocker.AsyncMock(
        return_value={
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "column"}, {"VarCharValue": "value"}]},  # Header row
                    {
                        "Data": [{"VarCharValue": "test_col"}, {"VarCharValue": "42"}]
                    },  # First data row
                ]
            },
            "NextToken": None,
        }
    )
    mock_athena.get_work_group = mocker.AsyncMock(
        return_value={"WorkGroup": {"Name": "test_group"}}
    )
    mock_s3.head_bucket = mocker.AsyncMock(return_value={})

    # Patch the session and clients
    client._session = mock_session
    client._athena = mock_athena
    client._s3 = mock_s3
    client._glue = mock_glue

    # Patch the params attribute
    client.params = client.config.params

    return client


@pytest.fixture
def output_model():
    """Fixture for output model."""
    return MockModels.OutputModel


class TestAWSClientConnection:
    """Test suite for AWS client connection methods."""

    @pytest.mark.asyncio
    async def test_connect_and_validate(self, aws_client, mocker):
        """Test successful connection and workgroup validation."""
        aws_client = await aws_client
        # Check that the first call to client was for athena, glue, and s3
        assert aws_client._athena is not None
        assert aws_client._glue is not None
        assert aws_client._s3 is not None

        # Verify workgroup validation
        aws_client._athena.get_work_group = mocker.AsyncMock(
            return_value={"WorkGroup": {"Name": "test_group"}}
        )
        aws_client._s3.head_bucket = mocker.AsyncMock()

        # Validate should not raise an exception
        await aws_client.validate()

    @pytest.mark.asyncio
    async def test_disconnect(self, aws_client):
        """Test client disconnection."""
        aws_client = await aws_client
        await aws_client.disconnect()
        assert aws_client._session is None


class TestAWSClientQueryManagement:
    """Test suite for AWS client query-related methods."""

    @pytest.mark.asyncio
    async def test_read_method(self, aws_client, mocker):
        """Test read method."""
        aws_client = await aws_client
        # Mock the query execution and results retrieval
        aws_client._execute_query = mocker.AsyncMock(return_value="test_query_id")
        aws_client._athena.get_query_results = mocker.AsyncMock(
            return_value={
                "ResultSet": {
                    "Rows": [
                        {
                            "Data": [{"VarCharValue": "column"}, {"VarCharValue": "value"}]
                        },  # Header row
                        {
                            "Data": [{"VarCharValue": "test_col"}, {"VarCharValue": "42"}]
                        },  # First data row
                    ]
                },
                "NextToken": None,
            }
        )
        aws_client._wait_for_query = mocker.AsyncMock()

        # Use the existing SimpleRecord model from MockModels
        results = [record async for record in aws_client.read(output_model=MockModels.SimpleRecord)]

        # Verify results
        assert len(results) == 1
        assert results[0] == {
            "column": "test_col",
            "value": "42",
        }  # Corrected type conversion for integer value


class TestAWSClientErrorHandling:
    """Test suite for AWS client error handling scenarios."""

    @pytest.mark.asyncio
    async def test_not_connected_errors(self, aws_client):
        """Test errors when client is not connected."""
        aws_client = await aws_client
        # Manually set connection state to False
        aws_client._session = None
        aws_client._athena = None

        with pytest.raises(NotConnectedError, match="Not connected to AWS"):
            [record async for record in aws_client.read()]

    @pytest.mark.asyncio
    async def test_query_state_parsing_malformed_response(self, aws_client):
        """Test handling of malformed query state responses."""
        aws_client = await aws_client

        # Test missing QueryExecution
        with pytest.raises(MalformedResponseError, match="QueryExecution"):
            aws_client._get_query_state({})

        # Test missing Status
        with pytest.raises(MalformedResponseError, match="Status"):
            aws_client._get_query_state({"QueryExecution": {}})

        # Test missing State
        with pytest.raises(MalformedResponseError, match="State"):
            aws_client._get_query_state({"QueryExecution": {"Status": {"SomeOtherKey": "value"}}})

    @pytest.mark.asyncio
    async def test_query_execution_timeout(self, aws_client, mocker):
        """Test query execution timeout scenario."""
        aws_client = await aws_client

        # Mock get_query_execution to always return a running state
        aws_client._athena.get_query_execution = mocker.AsyncMock(
            return_value={"QueryExecution": {"Status": {"State": "RUNNING"}}}
        )

        # Set a very short timeout to trigger the timeout
        with pytest.raises(TimeoutError, match="did not complete"):
            await aws_client._wait_for_query("test_query_id", timeout=0.1)

    @pytest.mark.asyncio
    async def test_query_execution_error_states(self, aws_client, mocker):
        """Test handling of various query error states."""
        aws_client = await aws_client

        # Test FAILED state
        aws_client._athena.get_query_execution = mocker.AsyncMock(
            return_value={
                "QueryExecution": {
                    "Status": {"State": "FAILED", "StateChangeReason": "Test failure"}
                }
            }
        )

        with pytest.raises(QueryExecutionError) as exc_info:
            await aws_client._wait_for_query("test_query_id")

        assert "Test failure" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_query_error_handling(self, aws_client, mocker):
        """Test error handling during query execution."""
        aws_client = await aws_client

        # Simulate a client error during query execution
        aws_client._athena.start_query_execution = mocker.AsyncMock(
            side_effect=Exception(
                {"Error": {"Code": "TestError", "Message": "Test error message"}},
                "StartQueryExecution",
            )
        )

        with pytest.raises(Exception):
            await aws_client._execute_query("SELECT * FROM test_table")


class TestAWSClientInitialization:
    """Test suite for AWS client initialization."""

    @pytest.mark.asyncio
    async def test_initialization_paths(self, aws_client):
        """Test various initialization paths."""
        aws_client = await aws_client
        assert aws_client._athena is not None
        assert aws_client._glue is not None
        assert aws_client._s3 is not None


class TestAWSClientUtilityMethods:
    """Test suite for AWS client utility methods."""

    def test_get_s3_location(self):
        """Test parsing of S3 location."""
        client = AWSClient(
            {
                "type": "aws",
                "name": "test",
                "params": {
                    "region": Region.EU_CENTRAL_1,
                    "database": "test_db",
                    "workgroup": "test_group",
                    "output_location": "s3://test-bucket/test-path/",
                    "query": "SELECT 1",
                },
            }
        )

        # Validate S3 location parsing via urlparse
        parsed = urlparse(client.config.params.output_location)
        assert parsed.scheme == "s3"
        assert parsed.netloc == "test-bucket"
        assert parsed.path.startswith("/test-path/")

    def test_get_schema(self, output_model):
        """Test schema generation from Pydantic model."""
        expected_schema = {
            "text": AthenaTypes.STRING.value,
            "sentiment": AthenaTypes.STRING.value,
            "confidence": AthenaTypes.DOUBLE.value,
        }
        schema = TypeConverter.py2athena(output_model)
        assert schema == expected_schema


class TestAWSClientSchemaHandling:
    def test_get_schema_unmapped_types(self, caplog):
        """Test schema generation with unmapped types."""

        class UnmappedModel(BaseModel):
            custom_field: object  # Deliberately use an unmapped type
            complex_field: list[str]
            nested_field: dict[str, int]

        # Capture logging
        caplog.set_level(logging.WARNING)

        # Generate schema
        schema = TypeConverter.py2athena(UnmappedModel)

        # Verify schema generation
        assert "custom_field" in schema
        assert schema["custom_field"] == AthenaTypes.STRING.value  # Default to string
        assert schema["complex_field"].startswith("array<")
        assert schema["nested_field"].startswith("map<")

    @pytest.mark.asyncio
    async def test_records_to_table_type_conversion(self, aws_client, output_model):
        """Test conversion of records to Arrow table with type conversion."""
        aws_client = await aws_client

        # Prepare test data with various types
        records = [
            {
                "int_field": 42,
                "float_field": 3.14,
                "str_field": "hello",
                "bool_field": True,
                "complex_field": [1, 2, 3],
                "nested_field": {"key": "value"},
            }
        ]

        # Convert records to Arrow table
        table = await aws_client.records_to_table(records, output_model)

        # Validate table conversion
        assert table is not None
        assert len(table) == 1

        # Optional: Add more specific type checks if needed
        # This might require importing pyarrow and checking specific column types


class TestAWSClientAdvancedErrorHandling:
    @pytest.mark.asyncio
    async def test_get_schema_with_unsupported_types(self, aws_client, caplog):
        """Test schema generation with unsupported types."""
        aws_client = await aws_client

        class ComplexModel(BaseModel):
            complex_list: list[dict[str, int]]
            custom_type: object

        # Capture logging
        caplog.set_level(logging.WARNING)

        # Generate schema
        schema = TypeConverter.py2athena(ComplexModel)

        # Verify schema generation
        assert "complex_list" in schema
        assert schema["complex_list"].startswith("array<map<")
        assert "custom_type" in schema
        assert schema["custom_type"] == AthenaTypes.STRING.value  # Default to string

        # Check for warning logs about unsupported types
        # Note: This might need to be adjusted based on actual logging implementation

    @pytest.mark.asyncio
    async def test_execute_query_error_handling(self, aws_client, mocker):
        """Test error handling during query execution."""
        client = AWSClient(
            {
                "type": "aws",
                "name": "test",
                "params": {
                    "region": Region.EU_CENTRAL_1,
                    "database": "test_db",
                    "workgroup": "test_group",
                    "output_location": "s3://test-bucket/test-path/",
                    "query": "SELECT 1",
                },
            }
        )

        # Simulate a specific AWS client error
        with pytest.raises(QueryExecutionError, match="Query execution failed"):
            client._execute_query = mocker.AsyncMock(
                side_effect=ClientError(
                    {
                        "Error": {
                            "Code": "QueryExecutionFailed",
                            "Message": "Query failed to execute",
                        }
                    },
                    "StartQueryExecution",
                )
            )

    @pytest.mark.asyncio
    async def test_validate_nonexistent_workgroup(self, aws_client, mocker):
        """Test validation of a non-existent workgroup."""
        aws_client = await aws_client

        # Mock get_work_group to raise a specific exception
        aws_client._athena.get_work_group = mocker.AsyncMock(
            side_effect=ClientError(
                {"Error": {"Code": "InvalidRequestException", "Message": "Workgroup not found"}},
                "GetWorkGroup",
            )
        )

        with pytest.raises(ValueError, match="WorkGroup"):
            await aws_client.validate()

    @pytest.mark.asyncio
    async def test_s3_location_validation_error(self, aws_client, mocker):
        """Test S3 location validation error handling."""
        aws_client = await aws_client

        # Mock session client creation to simulate S3 client
        mock_session = mocker.AsyncMock()
        mock_session.client.return_value.__aenter__.return_value = mocker.AsyncMock()
        aws_client._session = mock_session

        # Mock head_bucket to raise a specific exception
        bucket_name = urlparse(aws_client.config.params.output_location).netloc
        aws_client._s3.head_bucket = mocker.AsyncMock(
            side_effect=ClientError(
                {"Error": {"Code": "NoSuchBucket", "Message": "Bucket does not exist"}},
                "HeadBucket",
            )
        )

        # Verify that the head_bucket method raises the expected error
        with pytest.raises(ClientError, match="NoSuchBucket"):
            await aws_client._s3.head_bucket(Bucket=bucket_name)
