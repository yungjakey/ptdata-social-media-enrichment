"""AWS type tests."""

import pytest

from src.connectors.aws.types import AWSParams, Region, TableConfig


def test_region_enum():
    """Test Region enum."""
    assert Region.EU_CENTRAL_1 == "eu-central-1"
    assert Region("eu-central-1") == Region.EU_CENTRAL_1


def test_table_config():
    """Test TableConfig validation and defaults."""
    # Test with filter
    config = TableConfig(name="test_table", filter="col = 'value'")
    assert config.name == "test_table"
    assert config.filter == "col = 'value'"

    # Test without filter
    config = TableConfig(name="test_table")
    assert config.name == "test_table"
    assert config.filter is None

    # Test validation
    with pytest.raises(TypeError):
        TableConfig()  # name is required


class TestAWSParams:
    """Test cases for AWSParams."""

    @pytest.fixture
    def valid_params(self):
        """Create valid AWS params for testing."""
        return {
            "database": "test_db",
            "region": Region.EU_CENTRAL_1,
            "output_location": "s3://bucket/path",
            "workgroup": "test_group",
            "tables": [TableConfig(name="test_table")],
            "query": "SELECT * FROM test_table",
        }

    def test_aws_params_creation(self, valid_params):
        """Test AWSParams creation with valid parameters."""
        params = AWSParams(**valid_params)
        assert params.database == "test_db"
        assert params.region == Region.EU_CENTRAL_1
        assert params.output_location == "s3://bucket/path"
        assert params.workgroup == "test_group"
        assert len(params.tables) == 1
        assert params.tables[0].name == "test_table"
        assert params.query == "SELECT * FROM test_table"
        assert params.wait_time == 10  # default
        assert params.max_retries == 3  # default
        assert params.max_wait_time == 60  # default

    def test_aws_params_validation(self, valid_params):
        """Test AWSParams validation."""
        # Test invalid database
        with pytest.raises(ValueError):
            params = valid_params.copy()
            params["database"] = ""  # Empty string should raise ValueError
            AWSParams(**params)

        # Test invalid region
        with pytest.raises(ValueError):
            params = valid_params.copy()
            params["region"] = "invalid-region"  # Invalid region should raise ValueError
            AWSParams(**params)

        # Test invalid workgroup
        with pytest.raises(ValueError):
            params = valid_params.copy()
            params["workgroup"] = ""  # Empty string should raise ValueError
            AWSParams(**params)

        # Test invalid S3 location
        with pytest.raises(ValueError):
            params = valid_params.copy()
            params["output_location"] = (
                "invalid-location"  # Invalid S3 location should raise ValueError
            )
            AWSParams(**params)

    def test_aws_params_from_dict(self, valid_params):
        """Test AWSParams.from_dict method."""
        params = AWSParams.from_dict(valid_params)
        assert isinstance(params, AWSParams)
        assert params.database == valid_params["database"]
        assert params.region == valid_params["region"]
        assert params.output_location == valid_params["output_location"]
        assert params.workgroup == valid_params["workgroup"]

    def test_aws_params_s3_location(self, valid_params):
        """Test S3 location validation."""
        # Test valid S3 locations
        valid_locations = [
            "s3://bucket/path",
            "s3://my-bucket/path/to/data/",
            "s3://bucket-123/path_123/data/",
        ]
        for location in valid_locations:
            params = valid_params.copy()
            params["output_location"] = location
            assert AWSParams(**params).output_location == location

        # Test invalid S3 locations
        invalid_locations = [
            "invalid-location",
            "s3:/bucket/path",  # Missing a slash
            "s3://",  # Missing bucket
            "s3://bucket",  # Missing path
            "file:///path",  # Wrong scheme
            "http://bucket/path",  # Wrong scheme
        ]
        for location in invalid_locations:
            with pytest.raises(ValueError):
                params = valid_params.copy()
                params["output_location"] = location
                AWSParams(**params)
