"""Test suite for Athena connector."""

import asyncio
import logging
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from src.connectors.athena.client import AWSClient, NotConnectedError, QueryExecutionError
from src.connectors.types import ConnectorConfig, ConnectorType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
def mock_session():
    """Mock AWS session."""
    with patch("boto3.Session") as mock:
        mock.return_value.client.return_value = MagicMock()
        yield mock


@pytest.fixture
def mock_athena_client(mock_session):
    """Mock Athena client with common test configuration."""
    config = ConnectorConfig(
        type=ConnectorType.ATHENA,
        name="test-connector",
        params={
            "database": "test_db",
            "region": "eu-central-1",
            "output_location": "s3://test-bucket/output/",
            "workgroup": "primary",
            "tables": [
                {
                    "name": "test_table",
                    "filter": "WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '1' DAY",
                }
            ],
            "query": "SELECT * FROM test_table",
        },
    )
    client = AWSClient(config)
    client.connect()
    return client


def test_connector_creation(mock_session):
    """Test connector creation from config."""
    config = ConnectorConfig(
        type=ConnectorType.ATHENA,
        name="test",
        params={
            "database": "test_db",
            "region": "eu-central-1",
            "output_location": "s3://test-bucket/",
            "workgroup": "primary",
            "tables": [],
            "query": "SELECT 1",
        },
    )

    client = AWSClient(config)
    assert client.type == ConnectorType.ATHENA
    assert client.name == "test"
    assert client.params["database"] == "test_db"


def test_validation():
    """Test configuration validation."""
    config = ConnectorConfig(type=ConnectorType.ATHENA, name="test", params={})

    client = AWSClient(config)
    with pytest.raises(ValueError, match="Missing required parameters"):
        client.validate()


@pytest.mark.asyncio
async def test_get_table_schema(mock_athena_client):
    """Test retrieving table schema from Glue."""
    # Mock Glue response
    mock_athena_client._glue.get_table.return_value = {
        "Table": {
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "id", "Type": "string"},
                    {"Name": "value", "Type": "int"},
                    {"Name": "timestamp", "Type": "timestamp"},
                ]
            }
        }
    }

    schema = await asyncio.to_thread(mock_athena_client.get_table_schema, "test_db", "test_table")

    assert schema["id"] == str
    assert schema["value"] == int
    assert schema["timestamp"] == str


@pytest.mark.asyncio
async def test_execute_query(mock_athena_client):
    """Test query execution."""
    mock_athena_client._athena.start_query_execution.return_value = {
        "QueryExecutionId": "test-query-id"
    }

    query_id = await mock_athena_client.execute_query("SELECT 1")
    assert query_id == "test-query-id"

    mock_athena_client._athena.start_query_execution.assert_called_with(
        QueryString="SELECT 1",
        QueryExecutionContext={"Database": "test_db"},
        ResultConfiguration={"OutputLocation": "s3://test-bucket/output/"},
        WorkGroup="primary",
    )


@pytest.mark.asyncio
async def test_read_data(mock_athena_client):
    """Test reading data from Athena."""
    # Mock query execution
    mock_athena_client._athena.start_query_execution.return_value = {
        "QueryExecutionId": "test-query-id"
    }

    # Mock query status
    mock_athena_client._athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }

    # Mock query results
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}, {"VarCharValue": "value"}]},  # Header
                    {"Data": [{"VarCharValue": "1"}, {"VarCharValue": "test"}]},  # Data
                ]
            }
        }
    ]
    mock_athena_client._athena.get_paginator.return_value = mock_paginator

    records = []
    async for record in mock_athena_client.read():
        records.append(record)

    assert len(records) == 1
    assert records[0] == {"id": "1", "value": "test"}


@pytest.mark.asyncio
async def test_write_data(mock_athena_client):
    """Test writing data to Athena."""

    async def mock_records():
        yield {"id": "1", "value": "test1"}
        yield {"id": "2", "value": "test2"}

    # Mock schema retrieval
    mock_athena_client._glue.get_table.return_value = {
        "Table": {
            "StorageDescriptor": {
                "Columns": [{"Name": "id", "Type": "string"}, {"Name": "value", "Type": "string"}]
            }
        }
    }

    # Mock query execution
    mock_athena_client._athena.start_query_execution.return_value = {
        "QueryExecutionId": "test-query-id"
    }

    # Mock query status
    mock_athena_client._athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }

    await mock_athena_client.write(mock_records())

    # Verify queries were executed
    calls = mock_athena_client._athena.start_query_execution.call_args_list
    assert len(calls) == 3  # CREATE TABLE, INSERT INTO, DROP TABLE

    # Verify CREATE TABLE
    create_call = calls[0][1]
    assert "CREATE TABLE" in create_call["QueryString"]
    assert "format = 'PARQUET'" in create_call["QueryString"]

    # Verify INSERT INTO
    insert_call = calls[1][1]
    assert "INSERT INTO" in insert_call["QueryString"]
    assert "SELECT * FROM" in insert_call["QueryString"]

    # Verify DROP TABLE
    drop_call = calls[2][1]
    assert "DROP TABLE" in drop_call["QueryString"]


@pytest.mark.asyncio
async def test_query_failure(mock_athena_client):
    """Test handling of failed queries."""
    mock_athena_client._athena.start_query_execution.return_value = {
        "QueryExecutionId": "test-query-id"
    }

    mock_athena_client._athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "FAILED", "StateChangeReason": "Test failure"}}
    }

    with pytest.raises(QueryExecutionError) as exc:
        await mock_athena_client.execute_query("SELECT 1")

    assert "Test failure" in str(exc.value)


def test_not_connected_error(mock_athena_client):
    """Test operations without connecting first."""
    client = AWSClient(mock_athena_client.config)  # Create new client without connecting

    with pytest.raises(NotConnectedError):
        client.get_table_schema("test_db", "test_table")


def test_client_error_handling(mock_athena_client):
    """Test AWS client error handling."""
    error_response = {"Error": {"Code": "TestError", "Message": "Test error message"}}
    mock_athena_client._glue.get_table.side_effect = ClientError(error_response, "GetTable")

    with pytest.raises(ClientError) as exc:
        mock_athena_client.get_table_schema("test_db", "test_table")

    assert "TestError" in str(exc.value)
    assert "Test error message" in str(exc.value)
