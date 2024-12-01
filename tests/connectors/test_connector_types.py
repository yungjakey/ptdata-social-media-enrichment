"""Test connector types."""

import pytest

from src.connectors.types import (
    Connector,
    ConnectorConfig,
    ConnectorProtocol,
    ConnectorType,
)


def test_connector_type():
    """Test connector type enum."""
    assert ConnectorType.ATHENA == "athena"
    with pytest.raises(ValueError):
        ConnectorType("invalid")


def test_connector_config():
    """Test connector configuration."""
    config = ConnectorConfig(
        type=ConnectorType.ATHENA,
        name="test",
        params={
            "database": "test_db",
            "region": "us-west-2",
            "output_location": "s3://test-bucket/output/",
            "workgroup": "test_group",
            "query": "SELECT * FROM test_table",
        },
    )
    assert config.type == ConnectorType.ATHENA
    assert config.name == "test"
    assert isinstance(config.params, dict)


def test_connector_protocol_abstract():
    """Test connector protocol abstract methods."""
    # Test that we can't instantiate protocol directly
    with pytest.raises(TypeError):
        ConnectorProtocol()

    # Test that we can't instantiate class missing required methods
    class InvalidConnector:
        type = ConnectorType.ATHENA
        name = "test"
        params = {}

    assert not isinstance(InvalidConnector(), ConnectorProtocol)

    # Test that we can instantiate class with all required methods
    class ValidConnector:
        type = ConnectorType.ATHENA
        name = "test"
        params = {}

        def validate(self): ...
        def connect(self): ...
        def disconnect(self): ...
        async def read(self): ...
        async def write(self, records): ...

    assert isinstance(ValidConnector(), ConnectorProtocol)


def test_abstract_method_implementations():
    """Test abstract method implementations."""

    class ConcreteConnector(ConnectorProtocol):
        async def connect(self):
            pass

        async def disconnect(self):
            pass

        async def write(self, records):
            pass

        async def read(self, query):
            pass

    # Test that concrete implementation works
    connector = ConcreteConnector()
    assert isinstance(connector, ConnectorProtocol)

    # Test that abstract class can't be instantiated
    with pytest.raises(TypeError):
        ConnectorProtocol()


def test_connector_registration():
    """Test connector registration."""

    # Test successful registration
    @Connector.register(ConnectorType.ATHENA)
    class TestConnector(ConnectorProtocol):
        def __init__(self, config):
            self.type = config.type
            self.name = config.name
            self.params = config.params

        def validate(self): ...
        def connect(self): ...
        def disconnect(self): ...
        async def read(self): ...
        async def write(self, records): ...

    assert Connector._registry[ConnectorType.ATHENA] == TestConnector


def test_connector_from_config():
    """Test connector creation from config."""

    # Test successful registration
    @Connector.register(ConnectorType.ATHENA)
    class TestConnector(ConnectorProtocol):
        def __init__(self, config):
            self.type = config.type
            self.name = config.name
            self.params = config.params

        def validate(self): ...
        def connect(self): ...
        def disconnect(self): ...
        async def read(self): ...
        async def write(self, records): ...

    # Test successful creation
    connector = Connector.from_config(
        type="athena",
        name="test",
        params={
            "database": "test_db",
            "region": "us-west-2",
            "output_location": "s3://test-bucket/output/",
            "workgroup": "test_group",
            "query": "SELECT * FROM test_table",
        },
    )
    assert isinstance(connector, TestConnector)
    assert connector.type == ConnectorType.ATHENA
    assert connector.name == "test"

    # Test invalid connector type
    with pytest.raises(ValueError, match="Unsupported connector type"):
        Connector.from_config(type="invalid", name="test", params={})

    # Test unregistered connector type
    Connector._registry.clear()  # Clear existing registrations
    with pytest.raises(ValueError, match="No connector registered for type"):
        Connector.from_config(type="athena", name="test", params={})
