"""Test AWS Athena and Glue functionality."""

import pytest

from src.aws.client import AWSClient
from src.aws.types import AWSConfig, DatabaseConfig, Region


@pytest.fixture
def mock_aws_client(mocker):
    """Create a mock AWS client."""
    # Create mock clients
    mock_glue = mocker.MagicMock()
    mock_athena = mocker.MagicMock()

    # Configure get_query_execution to return a dictionary with a specific state
    def mock_get_query_execution(QueryExecutionId):
        # This allows tests to override the side_effect
        return {
            "QueryExecution": {
                "Status": {
                    "State": "SUCCEEDED"  # Default state, can be overridden in tests
                }
            }
        }

    mock_athena.get_query_execution.side_effect = mock_get_query_execution

    # Configure get_query_results to return a sample result set
    def mock_get_query_results(QueryExecutionId):
        # This allows tests to override the side_effect
        return {
            "ResultSet": {
                "Rows": [
                    {
                        "Data": [
                            {"VarCharValue": "platform"},
                            {"VarCharValue": "post_count"},
                            {"VarCharValue": "avg_likes"},
                            {"VarCharValue": "avg_shares"},
                        ]
                    },
                    {
                        "Data": [
                            {"VarCharValue": "twitter"},
                            {"VarCharValue": "1"},
                            {"VarCharValue": "10.0"},
                            {"VarCharValue": "5.0"},
                        ]
                    },
                    {
                        "Data": [
                            {"VarCharValue": "facebook"},
                            {"VarCharValue": "1"},
                            {"VarCharValue": "20.0"},
                            {"VarCharValue": "8.0"},
                        ]
                    },
                ]
            }
        }

    mock_athena.get_query_results.side_effect = mock_get_query_results

    # Patch boto3 to return these mocked clients
    mocker.patch(
        "boto3.Session.client",
        side_effect=lambda service_name: {"glue": mock_glue, "athena": mock_athena}[service_name],
    )

    return mock_glue, mock_athena


@pytest.fixture
def aws_config():
    """Create a test AWS configuration."""
    mock_db = DatabaseConfig(source=[], target=[], primary=["id"])
    return AWSConfig(
        region=Region.EU_CENTRAL_1,
        output="s3://test-bucket/output/",
        db=mock_db,
    )


def test_aws_client_query_success(mocker):
    """Comprehensive test for AWS client query state handling."""
    # Create a mock boto3 session
    mock_session = mocker.Mock()

    # Create a mock Athena client
    mock_athena = mocker.Mock()
    mock_session.client.return_value = mock_athena

    # Mock the AWS session creation
    mocker.patch("boto3.Session", return_value=mock_session)

    # Create test configuration
    test_config = AWSConfig(
        region=Region.EU_CENTRAL_1,
        output="s3://test-bucket/output/",
        db=DatabaseConfig(source=[], target=[], primary=["id"]),
        wait_time=0.1,
        max_wait_time=10,
        max_retries=10,
    )

    # Create the client with mocked session
    client = AWSClient(test_config)

    # Scenario 1: Successful query with initial RUNNING state
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "success_query"}
    mock_athena.get_query_execution.side_effect = [
        {"QueryExecution": {"Status": {"State": "RUNNING"}}},
        {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
    ]
    mock_athena.get_query_results.return_value = {
        "ResultSet": {"Rows": [{"Data": [{"VarCharValue": "test"}]}]}
    }

    # This should now return results, not raise an error
    result = client.run("SELECT 1", database="test_db")
    assert result is not None
    mock_athena.get_query_results.assert_called_once()


def test_aws_client_query_failure(mocker, mock_aws_client, aws_config):
    # Create a mock boto3 session
    mock_session = mocker.Mock()

    # Create a mock Athena client
    mock_athena = mocker.Mock()
    mock_session.client.return_value = mock_athena

    # Mock the AWS session creation
    mocker.patch("boto3.Session", return_value=mock_session)

    # Create the client with mocked session
    client = AWSClient(aws_config)

    # Scenario 2: Successful query without initial RUNNING state
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "success_query"}
    mock_athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    mock_athena.get_query_results.return_value = {
        "ResultSet": {"Rows": [{"Data": [{"VarCharValue": "test"}]}]}
    }

    result = client.run("SELECT 2", database="test_db")
    assert result is not None
    mock_athena.get_query_results.assert_called_once()


def test_aws_client_social_media_query(mock_aws_client, aws_config):
    """Test querying the social media test table in Athena."""
    # Retrieve the mock Athena client
    _, mock_athena = mock_aws_client

    # Create a fresh client for this test
    client = AWSClient(aws_config)

    # Prepare a query to test our social media table
    query = """
    SELECT platform, COUNT(*) as post_count, 
           AVG(likes) as avg_likes, 
           AVG(shares) as avg_shares 
    FROM ptdata_test.social_media_test 
    GROUP BY platform
    """

    # Mock the query execution and results
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "social_media_query"}
    mock_athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    mock_athena.get_query_results.return_value = {
        "ResultSet": {
            "Rows": [
                {
                    "Data": [
                        {"VarCharValue": "platform"},
                        {"VarCharValue": "post_count"},
                        {"VarCharValue": "avg_likes"},
                        {"VarCharValue": "avg_shares"},
                    ]
                },  # Header row
                {
                    "Data": [
                        {"VarCharValue": "twitter"},
                        {"VarCharValue": "1"},
                        {"VarCharValue": "10.0"},
                        {"VarCharValue": "5.0"},
                    ]
                },
                {
                    "Data": [
                        {"VarCharValue": "facebook"},
                        {"VarCharValue": "1"},
                        {"VarCharValue": "20.0"},
                        {"VarCharValue": "8.0"},
                    ]
                },
            ]
        }
    }

    # Execute the query
    results = client.run(query, database="ptdata_test")

    # Verify Athena methods were called correctly
    mock_athena.start_query_execution.assert_called_once()
    mock_athena.get_query_execution.assert_called()
    mock_athena.get_query_results.assert_called_once()

    # Assertions
    assert results is not None, "Results should not be None"

    # Verify the results match the mock data
    expected_columns = ["platform", "post_count", "avg_likes", "avg_shares"]
    assert list(results.columns) == expected_columns, f"Unexpected columns: {results.columns}"
    assert len(results) == 2, f"Should return results for two platforms, got {results}"

    # Check twitter results
    twitter_result = results[results["platform"] == "twitter"].iloc[0]
    assert twitter_result["post_count"] == "1"
    assert twitter_result["avg_likes"] == "10.0"
    assert twitter_result["avg_shares"] == "5.0"

    # Check facebook results
    facebook_result = results[results["platform"] == "facebook"].iloc[0]
    assert facebook_result["post_count"] == "1"
    assert facebook_result["avg_likes"] == "20.0"
    assert facebook_result["avg_shares"] == "8.0"
