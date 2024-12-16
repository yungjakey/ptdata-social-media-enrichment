from typing import Literal

from pydantic import BaseModel, Field

from .base import BaseInferenceModel


class WissenNeed(BaseModel):
    type: Literal[
        "keep me engaged",
        "update me",
    ] = Field(
        ...,
        description="Specific need",
    )
    score: float = Field(
        ...,
        description="Relevance of specific need",
    )
    confidence: float = Field(
        ...,
        description="Confidence in analysis",
    )


class VerstehenNeed(BaseModel):
    type: Literal[
        "educate me",
        "give me perspective",
    ] = Field(
        ...,
        description="Specific need",
    )
    score: float = Field(
        ...,
        description="Relevance of specific need",
    )
    confidence: float = Field(
        ...,
        description="Confidence in analysis",
    )


class FuehlenNeed(BaseModel):
    type: Literal[
        "connect me",
        "help me",
    ] = Field(
        ...,
        description="Specific need",
    )
    score: float = Field(
        ...,
        description="Relevance of specific need",
    )
    confidence: float = Field(
        ...,
        description="Confidence in analysis",
    )


class MachenNeed(BaseModel):
    type: Literal[
        "inspire me",
        "divert me",
    ] = Field(
        ...,
        description="Specific need",
    )
    score: float = Field(
        ...,
        description="Relevance of specific need",
    )
    confidence: float = Field(
        ...,
        description="Confidence in analysis",
    )


class UserNeeds(BaseInferenceModel):
    wissen: WissenNeed = Field(
        ...,
        description="Wissens categories",
    )
    verstehen: VerstehenNeed = Field(
        ...,
        description="Verstehen categories",
    )
    fuehlen: FuehlenNeed = Field(
        ...,
        description="Fuehlen categories",
    )
    machen: MachenNeed = Field(
        ...,
        description="Machen categories",
    )
    summary: str = Field(
        ...,
        description="Summary of analysis",
    )

    @staticmethod
    def get_prompt():
        return (
            "You are a social media expert tasked with analyzing social media posts by viewing details and metrics. "
            "Analyze which needs the following social media post addresses and provide a needs analysis result. "
            "For each need category (wissen, verstehen, fuehlen, machen), provide: "
            "- The specific type from the allowed options"
            "- A score (0-1) indicating how strongly this need is addressed "
            "- A confidence score (0-1) for the total assessment "
            "Finally, provide an overall confidence score (0-1) and a summaryfor the entire analysis."
        )
