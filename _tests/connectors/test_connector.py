"""Test connector protocol."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from src.connectors.types import ConnectorProtocol, ConnectorType


class _MockModel(BaseModel):
    """Mock model for testing."""

    id: int
    name: str
    active: bool
    score: float


@pytest.fixture
def test_record():
    """Create test record."""
    return _MockModel(id=1, name="test", active=True, score=0.5)


@pytest.fixture
def mock_connector():
    """Create mock connector."""

    class MockConnector:
        def __init__(self):
            self.type = ConnectorType.ATHENA
            self.name = "test"
            self.params = {
                "database": "test_db",
                "region": "us-west-2",
                "output_location": "s3://test-bucket/output/",
                "workgroup": "test_group",
                "query": "SELECT * FROM test_table",
            }
            self._write_mock = AsyncMock()
            self.connect = MagicMock()
            self.disconnect = MagicMock()
            self.validate = MagicMock()

        async def read(self):
            """Mock read method."""
            records = [
                {"id": 1, "name": "test1", "active": True, "score": 0.5},
                {"id": 2, "name": "test2", "active": False, "score": 1.5},
            ]
            for record in records:
                yield record

        async def write(self, records, table_name, output_model):
            """Mock write method."""
            await self._write_mock(records, table_name, output_model)

    return MockConnector()


def test_connector_protocol(mock_connector: ConnectorProtocol):
    """Test connector protocol implementation."""
    assert isinstance(mock_connector, ConnectorProtocol)


@pytest.mark.asyncio
async def test_connector_write(mock_connector: ConnectorProtocol):
    """Test connector write method."""
    records = [
        {"id": 1, "name": "test1", "active": True, "score": 0.5},
        {"id": 2, "name": "test2", "active": False, "score": 1.5},
    ]

    # Write records
    await mock_connector.write(records, "test_table", _MockModel)


@pytest.mark.asyncio
async def test_connector_read(mock_connector: ConnectorProtocol):
    """Test connector read method."""
    # Read records
    records = []
    async for record in mock_connector.read():
        assert isinstance(record, dict)
        assert all(isinstance(k, str) for k in record.keys())
        records.append(record)

    # Verify we got some records
    assert len(records) > 0


def test_connector_registration():
    """Test connector registration."""
    from src.connectors.types import Connector, ConnectorProtocol, ConnectorType

    # Test successful registration
    @Connector.register(ConnectorType.ATHENA)
    class TestConnector(ConnectorProtocol):
        async def validate(self): ...
        async def connect(self): ...
        async def disconnect(self): ...
        async def read(self): ...
        async def write(self, records): ...

    assert Connector._registry[ConnectorType.ATHENA] == TestConnector


def test_connector_from_config():
    """Test connector creation from config."""
    from src.connectors.types import Connector

    # Test invalid connector type
    with pytest.raises(ValueError, match="Unsupported connector type: invalid"):
        Connector.from_config("invalid", "test", {})

    # Test unregistered connector type
    Connector._registry.clear()  # Clear existing registrations
    with pytest.raises(ValueError, match="No connector registered for type: athena"):
        Connector.from_config("athena", "test", {})
