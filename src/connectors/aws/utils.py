from __future__ import annotations

from enum import Enum, auto
from typing import Any


class QueryState(Enum):
    """Enumeration of possible Athena query states."""

    QUEUED = auto()
    RUNNING = auto()
    SUCCEEDED = auto()
    FAILED = auto()
    CANCELLED = auto()

    @classmethod
    def terminal_states(cls) -> set[QueryState]:
        """Return terminal states."""
        return {cls.SUCCEEDED, cls.FAILED, cls.CANCELLED}

    @classmethod
    def running_states(cls) -> set[QueryState]:
        """Return running states."""
        return {cls.QUEUED, cls.RUNNING}


class NotConnectedError(Exception):
    """Raised when trying to use client before connecting."""


class S3LocationError(Exception):
    """Raised when S3 location is invalid."""


class QueryExecutionError(Exception):
    """Raised when a query execution fails."""

    def __init__(self, query_id: str, state: QueryState, reason: str | None = None):
        self.query_id = query_id
        self.state = state
        self.reason = reason
        message = f"Query {query_id} failed with state {state.name}"
        if reason:
            message += f": {reason}"
        super().__init__(message)


class MalformedResponseError(Exception):
    """Raised when AWS response is missing required fields."""

    def __init__(self, response: dict[str, Any], missing_field: str):
        self.response = response
        self.missing_field = missing_field
        super().__init__(f"AWS response missing required field: {missing_field}")
