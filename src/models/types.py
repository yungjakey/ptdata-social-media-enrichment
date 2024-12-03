"""Dynamic model builder."""

from pydantic import BaseModel, ConfigDict, Field


class ModelConfig(BaseModel):
    """Model configuration."""

    prompt: str = Field(..., min_length=1, description="Prompt template")
    schema: dict[str, type] = Field(..., description="Model schema")

    model_config = ConfigDict(extra="forbid", validate_assignment=True)
