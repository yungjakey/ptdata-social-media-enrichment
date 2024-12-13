from enum import Enum
from typing import Literal

from pydantic import Field

from .base import BaseInferenceModel


class Needs(str, Enum):
    wissen = Literal["keep me engaged", "update me"]
    verstehen = Literal["educate me", "give me perspective"]
    fühlen = Literal["connect me", "help me"]
    machen = Literal["inspire me", "divert me"]


class UserNeeds(BaseInferenceModel):
    need: Needs
    score: float = Field(ge=0, le=1)
    confidence: float = Field(ge=0, le=1)

    @staticmethod
    def get_prompt():
        return (
            "You are a social media expert tasked with analyzing social media posts by viewing details and metrics. "
            "Analyze the needs of the following social media post and provide a needs analysis result. "
            "The needs analysis result should include the needs, needs score, and confidence. "
            "The needs score ranges from 0 to 1, where 0 indicates no needs, and 1 indicates very high needs. "
            "The confidence ranges from 0 to 1, where 0 indicates no confidence, and 1 indicates very high confidence."
        )
