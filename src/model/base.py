"""Base models for social media analytics."""
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class BaseModel:
    """Base model for all data models."""
    id: bytes
    post_key: str
    evaluation_time: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert instance to dictionary."""
        return {
            "id": self.id,
            "post_key": self.post_key,
            "evaluation_time": self.evaluation_time
        }
