from .component import ConnectorComponent
from .config import ConnectorConfig

_REGISTRY = {
    "aws",
}


class ConnectorFactory:
    @staticmethod
    def get_connector(name: str) -> type[ConnectorComponent]:
        if name not in _REGISTRY:
            raise ValueError(f"Unknown connector: {name}")

        module_path = f"src.connectors.{name.lower()}.component"
        connector_name = f"{name.upper()}Connector"
        connector_module = __import__(module_path, fromlist=[connector_name])

        return getattr(connector_module, connector_name)

    @staticmethod
    def from_config(
        input: dict[str, str],
        output: dict[str, str],
    ) -> ConnectorComponent:
        try:
            input_config = ConnectorConfig(**input)
        except Exception as e:
            raise ValueError(f"Error parsing input config: {e}") from e

        try:
            output_config = ConnectorConfig(**output)
        except Exception as e:
            raise ValueError(f"Error parsing output config: {e}") from e

        try:
            input_connector = ConnectorFactory.get_connector(input_config.provider)
        except Exception as e:
            raise ValueError(f"Error creating input connector: {e}") from e

        try:
            output_connector = ConnectorFactory.get_connector(output_config.provider)
        except Exception as e:
            raise ValueError(f"Error creating output connector: {e}") from e

        return input_connector(input_config), output_connector(output_config)
