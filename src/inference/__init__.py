"""Azure package for social media analytics."""

from .client import OpenAIClient
from .types import OpenAIConfig

__all__ = ["OpenAIClient", "OpenAIConfig"]
