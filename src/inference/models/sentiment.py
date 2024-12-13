from typing import Literal

from pydantic import Field

from .base import BaseInferenceModel


class Sentiment(BaseInferenceModel):
    """Sentiment analysis result."""

    score: float = Field(
        ...,
        description="Sentiment score",
    )
    sentiment: Literal[
        "positive",
        "negative",
        "neutral",
    ] = Field(..., description="Analysis summary")
    confidence: float = Field(
        ...,
        description="Analysis confidence",
    )

    @staticmethod
    def get_prompt():
        return (
            "You are a social media expert tasked with analyzing social media posts by viewing details and metrics. "
            "Analyze the sentiment of the following social media post and provide a sentiment analysis result. "
            "The sentiment analysis result should include the sentiment score, post impact, sentiment summary, and confidence. "
            "The sentiment score ranges from -1 to 1, where -1 indicates a very negative sentiment, 0 indicates neutral sentiment, and 1 indicates a very positive sentiment. "
            "The sentiment summary should be a short summary of the sentiment analysis result. "
            "The confidence ranges from 0 to 1, where 0 indicates no confidence, and 1 indicates very high confidence."
        )
