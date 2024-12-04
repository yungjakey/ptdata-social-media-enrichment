from __future__ import annotations

from enum import Enum
from typing import Any


class QueryState(str, Enum):
    """Athena query states.
    Reference: https://docs.aws.amazon.com/athena/latest/APIReference/API_QueryExecutionStatus.html
    """

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @classmethod
    def terminal_states(cls) -> set[QueryState]:
        """States where no further status changes will occur."""
        return {cls.SUCCEEDED, cls.FAILED, cls.CANCELLED}

    @classmethod
    def running_states(cls) -> set[QueryState]:
        """States where query is still processing."""
        return {cls.QUEUED, cls.RUNNING}

    @classmethod
    def from_response(cls, status: dict[str, Any]) -> QueryState:
        """Create QueryState from Athena API response."""
        try:
            state = status["State"]
            return cls(state)
        except (KeyError, ValueError) as e:
            raise Exception(f"Error parsing Athena query state: {status}.") from e
