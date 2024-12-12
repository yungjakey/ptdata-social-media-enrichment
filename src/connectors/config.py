"""AWS connector configuration."""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


class TableConfig(BaseModel):
    """Common configuration for tables."""

    database: str = Field(
        ...,
        description="Database name",
    )
    table: str = Field(
        ...,
        description="Table name",
    )
    index_field: str | None = Field(
        ...,
        description="Field for identifying records",
    )
    datetime_field: str = Field(
        ...,
        description="Field for tracking updates",
    )
    location: str | None = Field(
        default=None,
        description="S3 location for table",
    )
    partition_by: str | None = Field(
        default=None,
        description="Fields to partition by",
    )


class SourceConfig(BaseModel):
    """Configuration for source data."""

    tables: list[TableConfig] = Field(
        ...,
        description="Tables to read from",
    )
    time_filter_hours: int | None = Field(
        default=None, description="Only process records updated within this time window", ge=1
    )

    @model_validator(mode="after")
    def validate_index_fields_match(self) -> SourceConfig:
        """Validate that all tables use the same index field."""
        if not self.tables:
            return self

        index_field = self.tables[0].index_field
        mismatched = [t for t in self.tables if t.index_field != index_field]

        if mismatched:
            tables = ", ".join(f"{t.database}.{t.table}" for t in mismatched)
            raise ValueError(
                f"All source tables must use the same index field. "
                f"Expected '{index_field}' but found mismatches in: {tables}"
            )
        return self


class TargetConfig(TableConfig):
    """Configuration for target data."""

    ...


class AWSConnectorConfig(BaseModel):
    """AWS connector configuration."""

    region: str = Field(
        default="eu-central-1",
        description="AWS region",
    )
    source: SourceConfig = Field(
        ...,
        description="Source configuration",
    )
    target: TargetConfig = Field(
        ...,
        description="Target configuration",
    )
