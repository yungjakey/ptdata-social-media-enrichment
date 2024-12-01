"""Pytest configuration."""

import sys
from unittest.mock import AsyncMock

import path
import pytest
from pydantic import BaseModel

from src.connectors.aws.types import AWSParams

sys.path.append(str(path.Path(__file__).parent.parent))

pytest_plugins = ["pytest_asyncio"]


@pytest.fixture
def config() -> AWSParams:
    """Create a test AWS config."""
    return AWSParams(
        database="test_db",
        region="us-east-1",
        output_location="s3://test-bucket/test-path/",
        workgroup="test_workgroup",
    )


class BaseConnectorMockFixture:
    """Base fixture for creating mock connectors with common behaviors."""

    @classmethod
    def create_mock_config(cls, **kwargs):
        """Create a standard test configuration with optional overrides."""
        default_config = {
            "type": "test",
            "name": "test_connector",
            "params": {
                "database": "test_db",
                "region": "us-west-2",
                "output_location": "s3://test-bucket/output/",
                "workgroup": "test_group",
                "query": "SELECT * FROM test_table",
            },
        }
        default_config.update(kwargs)
        return default_config

    @classmethod
    def create_base_mock_session(cls, service_name=None):
        """Create a base mock session with optional service-specific mocking."""
        session = AsyncMock()

        if service_name:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            session.client = AsyncMock(return_value=mock_client)

        return session

    @classmethod
    def create_mock_model(cls, **fields):
        """Dynamically create a mock Pydantic model for testing."""
        default_fields = {"id": (int, ...), "name": (str, ...)}
        default_fields.update(fields)

        return type("MockModel", (BaseModel,), {k: (v[0], v[1]) for k, v in default_fields.items()})


@pytest.fixture
def base_mock_connector():
    """Fixture to create a base mock connector."""

    class MockConnector:
        def __init__(self, config=None):
            self.type = "test"
            self.name = "test_connector"
            self.params = config or BaseConnectorMockFixture.create_mock_config()

            # Common mocked methods
            self._write_mock = AsyncMock()
            self.connect = AsyncMock()
            self.disconnect = AsyncMock()
            self.validate = AsyncMock()

        async def read(self):
            """Default mock read method."""
            records = [
                {"id": 1, "name": "test1"},
                {"id": 2, "name": "test2"},
            ]
            for record in records:
                yield record

        async def write(self, records, table_name, output_model):
            """Default mock write method."""
            await self._write_mock(records, table_name, output_model)

    return MockConnector()
