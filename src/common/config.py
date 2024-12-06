from typing import Any, TypeVar

from pydantic import BaseModel, ConfigDict, Field


class BaseConfig(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        from_attributes=True,
    )


class RootConfig(BaseConfig):
    connector: dict[str, Any] = Field(..., description="Connector configuration")
    inference: dict[str, Any] = Field(..., description="Inference configuration")
    logging: dict[str, Any] | None = Field(default=None, description="Logging configuration")


TConf = TypeVar("TConf", bound=BaseConfig)
