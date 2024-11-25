from __future__ import annotations

import json

from pandas import DataFrame

from src.common.config import ModelConfig


class Payload:
    """OpenAI API payload wrapper."""

    def __init__(self, system: str, user: str, config: dict[str, type]):
        """Initialize payload with messages and config."""
        self.messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        self.config = ModelConfig(**config)

    def __str__(self) -> str:
        """String representation of payload."""
        return json.dumps(self.__dict__(), indent=2)

    def __dict__(self) -> dict[str, type]:
        """Convert to dictionary format."""
        return {
            "messages": self.messages,
            **self.config.to_dict(),
        }


class Table:
    """Table metadata and schema mapping from Glue."""

    def __init__(self, doc: str = "", columns: list[dict] = None) -> None:
        """Initialize Table with documentation and column metadata."""
        self.doc = doc.strip()
        self.columns = columns or []

    @classmethod
    def from_glue(cls, doc: str, table: dict[str, type]) -> Table:
        """Create a Table instance from Glue table metadata."""
        storage_desc = table.get("StorageDescriptor", {})
        columns = storage_desc.get("Columns", [])
        return cls(doc=doc, columns=columns)

    def get_schema(self) -> dict[str, type]:
        """Extract schema from Glue table metadata."""
        schema = {}
        for col in self.columns:
            name = col.get("Name")
            type_str = col.get("Type")
            if name and type_str:
                schema[name] = self._GLUE2PY.get(type_str, "str")
        return schema

    def to_dataframe(self) -> DataFrame:
        """Convert table data to DataFrame if available."""
        if not self.columns:
            return DataFrame()

        column_names = [col.get("Name") for col in self.columns if col.get("Name")]
        return DataFrame(columns=column_names)
