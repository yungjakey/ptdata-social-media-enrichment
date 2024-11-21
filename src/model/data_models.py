"""Data models for social media analytics."""
from dataclasses import dataclass
from typing import Any, Optional

@dataclass
class ProcessingResult:
    """Result of processing a social media record."""
    facts: dict[str, Any]
    dims: dict[str, Any]
