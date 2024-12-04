from __future__ import annotations

from typing import TypeVar

from pydantic import BaseModel, ConfigDict


class BaseConfig(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        from_attributes=True,
    )


# Type hint for generic config types
TConf = TypeVar("TConf", bound=BaseConfig)
