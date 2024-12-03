"""AWS type tests."""

import pytest

from src.connectors.aws.types import AWSParams, Region


def test_region_enum():
    """Test Region enum."""
    assert Region.EU_CENTRAL_1 == "eu-central-1"
    assert Region("eu-central-1") == Region.EU_CENTRAL_1


class TestAWSParams:
    """Test cases for AWSParams."""

    @pytest.fixture
    def valid_params(self):
        """Create valid AWS params for testing."""
        return {
            "database": "test_db",
            "query": "SELECT * FROM test_table",
            "region": Region.EU_CENTRAL_1,
            "output_location": "s3://bucket/path",
            "workgroup": "test_group",
        }

    def test_aws_params_creation(self, valid_params):
        """Test AWSParams creation with valid parameters."""
        params = AWSParams(**valid_params)
        assert params.database == "test_db"
        assert params.region == Region.EU_CENTRAL_1
        assert params.output_location == "s3://bucket/path"
        assert params.workgroup == "test_group"
