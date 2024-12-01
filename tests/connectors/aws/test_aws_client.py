"""AWS Client Test Suite.

This module contains comprehensive tests for the AWS Athena client implementation.
"""

from urllib.parse import urlparse

import pyarrow as pa
import pytest
from pydantic import BaseModel

from src.connectors.aws.client import (
    AWSClient,
    ClientError,
    MalformedResponseError,
    NotConnectedError,
    QueryExecutionError,
    S3LocationError,
)
from src.connectors.aws.types import Region, TableConfig
from src.connectors.types import ConnectorConfig


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
    def create_config() -> ConnectorConfig:
        """Create a standard test configuration for AWS client."""
        params = {
            "database": "test_db",
            "region": Region.EU_CENTRAL_1.value,
            "output_location": "s3://bucket/path",
            "workgroup": "test_group",
            "tables": [TableConfig(name="test_table")],
            "query": "SELECT * FROM test_table",
        }
        return ConnectorConfig(type="aws", name="test", params=params)


@pytest.fixture
async def aws_client(mocker):
    """Create a test AWS client with mocked dependencies."""
    config = AWSClientFixtures.create_config()
    client = AWSClient(config)

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
                "WorkGroup": "test_workgroup",
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
        return_value={"WorkGroup": {"Name": "test_workgroup"}}
    )

    # Ensure head_bucket is called with the correct bucket name
    mock_s3.head_bucket = mocker.AsyncMock(return_value={})
    mock_s3.list_objects_v2 = mocker.AsyncMock(return_value={"Contents": [{"Key": "test_key"}]})

    # Replace client's session and clients with mocks
    client._session = mock_session
    client._athena = mock_athena
    client._s3 = mock_s3
    client._glue = mock_glue

    # Simulate connection
    await client.connect()

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
        # Check that the first call to client was for athena
        aws_client._session.client.assert_has_calls(
            [mocker.call("athena"), mocker.call("glue"), mocker.call("s3")]
        )
        # Update to use the full bucket name from output_location
        bucket_name = urlparse(aws_client.params.output_location).netloc
        aws_client._s3.head_bucket.assert_called_once_with(Bucket=bucket_name)

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
        aws_client._athena.get_query_results.return_value = {
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

        results = [record async for record in aws_client.read()]
        assert len(results) == 1, "Expected one record after processing"
        assert results[0]["column"] == "test_col", "First column value incorrect"
        assert results[0]["value"] == 42, "Value should be converted to integer"


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
            side_effect=ClientError(
                {"Error": {"Code": "TestError", "Message": "Test error message"}},
                "StartQueryExecution",
            )
        )

        with pytest.raises(ClientError):
            await aws_client._execute_query("SELECT * FROM test_table")

    @pytest.mark.asyncio
    async def test_s3_location_parsing(self, aws_client):
        """Test S3 location parsing."""
        aws_client = await aws_client
        location = "s3://test-bucket/test-path/file.csv"
        parsed = aws_client._get_s3_location(location)
        assert parsed[0] == "test-bucket"
        assert parsed[1] == "test-path/file.csv"

    @pytest.mark.asyncio
    async def test_s3_location_error_handling(self, aws_client):
        """Test S3 location parsing error handling."""
        aws_client = await aws_client

        # Test invalid S3 location schemes
        invalid_locations = [
            "http://test-bucket/path",
            "file:///test/path",
            "gs://test-bucket/path",
        ]

        for location in invalid_locations:
            with pytest.raises(S3LocationError, match="Invalid S3 location"):
                aws_client._get_s3_location(location)


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
            ConnectorConfig(
                type="aws",
                name="test",
                params={
                    "region": "eu-central-1",
                    "database": "test",
                    "workgroup": "test",
                    "output_location": "s3://test-bucket/test-path/",
                },
            )
        )

        # Valid S3 location
        bucket, key = client._get_s3_location("s3://mybucket/mypath/file.txt")
        assert bucket == "mybucket"
        assert key == "mypath/file.txt"

        # Invalid S3 location
        with pytest.raises(S3LocationError):
            client._get_s3_location("http://invalid-location")

    def test_get_schema(self, output_model):
        """Test schema generation from Pydantic model."""
        client = AWSClient(
            ConnectorConfig(
                type="aws",
                name="test",
                params={
                    "region": "eu-central-1",
                    "database": "test",
                    "workgroup": "test",
                    "output_location": "s3://test-bucket/test-path/",
                },
            )
        )

        schema = client._get_schema(output_model)

        # Verify expected schema mapping
        assert schema == {"text": "string", "sentiment": "string", "confidence": "double"}

    def test_records_to_table(self, output_model):
        """Test conversion of records to Arrow table."""
        client = AWSClient(
            ConnectorConfig(
                type="aws",
                name="test",
                params={
                    "region": "eu-central-1",
                    "database": "test",
                    "workgroup": "test",
                    "output_location": "s3://test-bucket/test-path/",
                },
            )
        )

        records = [
            {"text": "Test 1", "sentiment": "positive", "confidence": 0.95},
            {"text": "Test 2", "sentiment": "negative", "confidence": 0.05},
        ]

        schema = client._get_schema(output_model)
        table = client._records_to_table(records, schema)

        assert table is not None
        assert table.num_rows == 2
        assert table.num_columns == 3


class TestAWSClientSchemaHandling:
    """Test suite for AWS client schema-related methods."""

    @pytest.mark.asyncio
    async def test_get_schema_unmapped_types(self, aws_client, caplog):
        """Test schema generation with unmapped types."""
        aws_client = await aws_client

        class ModelWithUnmappedType(BaseModel):
            """Model with an unmapped type."""

            complex_field: list[str]
            nested_field: dict[str, int]

        # Capture warning logs
        schema = aws_client._get_schema(ModelWithUnmappedType)

        # Check warning logs
        assert any("unmapped type" in record.message for record in caplog.records)

        # Verify that unmapped types default to string
        assert schema["complex_field"] == "string"
        assert schema["nested_field"] == "string"

    def test_records_to_table_type_conversion(self, aws_client, output_model, event_loop):
        """Test conversion of records to Arrow table with type conversion."""
        # Use event_loop to run the async fixture
        aws_client = event_loop.run_until_complete(aws_client)

        records = [
            {"text": "Test", "sentiment": "positive", "confidence": 0.95},
            {"text": "Another", "sentiment": "negative", "confidence": 0.2},
        ]

        schema = {"text": "string", "sentiment": "string", "confidence": "double"}

        table = aws_client._records_to_table(records, schema)

        # Verify table properties
        assert table.num_rows == 2
        assert table.column("text").type == pa.string()
        assert table.column("confidence").type == pa.float64()

        # Verify data integrity
        assert table.column("text")[0].as_py() == "Test"
        assert table.column("confidence")[1].as_py() == 0.2


class TestAWSClientAdvancedErrorHandling:
    """Advanced test suite for AWS client error handling scenarios."""

    @pytest.mark.asyncio
    async def test_validate_nonexistent_workgroup(self, aws_client, mocker):
        """Test validation of a non-existent workgroup."""
        aws_client = await aws_client

        # Mock get_work_group to raise a ClientError
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

        # Mock head_bucket to raise a ClientError
        aws_client._session.client.return_value.__aenter__.return_value.head_bucket = (
            mocker.AsyncMock(
                side_effect=ClientError(
                    {"Error": {"Code": "NoSuchBucket", "Message": "Bucket does not exist"}},
                    "HeadBucket",
                )
            )
        )

        with pytest.raises(ClientError):
            await aws_client.connect()

    @pytest.mark.asyncio
    async def test_query_execution_with_empty_results(self, aws_client, mocker):
        """Test handling of query execution with empty result set."""
        aws_client = await aws_client

        # Mock query execution and results retrieval with an empty result set
        query_id = "test_empty_query_id"
        aws_client._athena.start_query_execution = mocker.AsyncMock(
            return_value={"QueryExecutionId": query_id}
        )
        aws_client._athena.get_query_execution = mocker.AsyncMock(
            return_value={
                "QueryExecution": {
                    "Status": {"State": "SUCCEEDED", "CompletionDateTime": "2023-01-01T00:00:00Z"}
                }
            }
        )
        aws_client._athena.get_query_results = mocker.AsyncMock(
            return_value={
                "ResultSet": {
                    "Rows": [],  # Empty result set
                    "ResultSetMetadata": {"ColumnInfo": []},
                },
                "NextToken": None,
            }
        )

        # Execute the query and verify empty results
        results = [record async for record in aws_client.read()]
        assert len(results) == 0, "Expected empty result set"

    @pytest.mark.asyncio
    async def test_query_execution_with_multiple_pages(self, aws_client, mocker):
        """Test handling of query results with multiple pages."""
        aws_client = await aws_client

        # Mock query execution and multi-page results
        query_id = "test_multi_page_query_id"
        aws_client._athena.start_query_execution = mocker.AsyncMock(
            return_value={"QueryExecutionId": query_id}
        )
        aws_client._athena.get_query_execution = mocker.AsyncMock(
            return_value={
                "QueryExecution": {
                    "Status": {"State": "SUCCEEDED", "CompletionDateTime": "2023-01-01T00:00:00Z"}
                }
            }
        )
        aws_client._athena.get_query_results = mocker.AsyncMock(
            side_effect=[
                {
                    "ResultSet": {
                        "Rows": [
                            {
                                "Data": [{"VarCharValue": "column1"}, {"VarCharValue": "column2"}]
                            },  # Header
                            {"Data": [{"VarCharValue": "value1"}, {"VarCharValue": "data1"}]},
                        ]
                    },
                    "NextToken": "page1_token",
                },
                {
                    "ResultSet": {
                        "Rows": [{"Data": [{"VarCharValue": "value2"}, {"VarCharValue": "data2"}]}]
                    },
                    "NextToken": None,
                },
            ]
        )

        # Execute the query and verify results
        results = [record async for record in aws_client.read()]
        assert len(results) == 2, "Expected two records across multiple pages"
        assert results[0] == {
            "column1": "value1",
            "column2": "data1",
        }, "First page result incorrect"
        assert results[1] == {
            "column1": "value2",
            "column2": "data2",
        }, "Second page result incorrect"

    @pytest.mark.asyncio
    async def test_get_schema_with_unsupported_types(self, aws_client):
        """Test schema generation with unsupported types."""
        aws_client = await aws_client

        class ModelWithUnsupportedTypes(BaseModel):
            """Model with complex and unsupported types."""

            complex_list: list[dict[str, int]]
            custom_type: object

        # Capture warning logs
        with pytest.warns(UserWarning, match="unmapped type"):
            schema = aws_client._get_schema(ModelWithUnsupportedTypes)

        # Verify that unsupported types default to string
        assert schema["complex_list"] == "string"
        assert schema["custom_type"] == "string"
