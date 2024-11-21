"""Output models for social media analytics."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from src.model.base import BaseModel


@dataclass
class AiPostMetrics(BaseModel):
    """AI-generated metrics for a social media post."""
    sentiment_score: float
    engagement_score: float
    reach_score: float
    influence_score: float
    overall_impact: float
    confidence: float

    def to_dict(self) -> dict[str, Any]:
        """Convert instance to dictionary."""
        base_dict = super().to_dict()
        base_dict.update({
            "sentiment_score": self.sentiment_score,
            "engagement_score": self.engagement_score,
            "reach_score": self.reach_score,
            "influence_score": self.influence_score,
            "overall_impact": self.overall_impact,
            "confidence": self.confidence
        })
        return base_dict


@dataclass
class AiPostDetails(BaseModel):
    """AI-generated detailed analysis for a social media post."""
    key_topics: list[str]
    content_category: str
    audience_type: str
    tone: str
    recommendations: list[str]
    additional_insights: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert instance to dictionary."""
        base_dict = super().to_dict()
        base_dict.update({
            "key_topics": self.key_topics,
            "content_category": self.content_category,
            "audience_type": self.audience_type,
            "tone": self.tone,
            "recommendations": self.recommendations,
            "additional_insights": self.additional_insights
        })
        return base_dict


@dataclass
class SocialMediaEvaluation:
    """Combined evaluation results for a social media post."""
    metrics: AiPostMetrics
    details: AiPostDetails

    def to_dict(self) -> dict[str, Any]:
        """Convert instance to dictionary."""
        return {
            "metrics": self.metrics.to_dict(),
            "details": self.details.to_dict()
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any], input_data: dict[str, Any]) -> "SocialMediaEvaluation":
        """Create instance from dictionary data."""
        metrics = AiPostMetrics(
            id=input_data.get("id"),
            post_key=input_data.get("post_key"),
            evaluation_time=datetime.now(),
            sentiment_score=data["metrics"]["sentiment_score"],
            engagement_score=data["metrics"]["engagement_score"],
            reach_score=data["metrics"]["reach_score"],
            influence_score=data["metrics"]["influence_score"],
            overall_impact=data["metrics"]["overall_impact"],
            confidence=data["metrics"]["confidence"]
        )

        details = AiPostDetails(
            id=input_data.get("id"),
            post_key=input_data.get("post_key"),
            evaluation_time=datetime.now(),
            key_topics=data["details"]["key_topics"],
            content_category=data["details"]["content_category"],
            audience_type=data["details"]["audience_type"],
            tone=data["details"]["tone"],
            recommendations=data["details"]["recommendations"],
            additional_insights=data["details"].get("additional_insights")
        )

        return cls(metrics=metrics, details=details)


@dataclass
class BatchEvaluationResult:
    """Results from processing a batch of social media posts."""
    successful_evaluations: list[SocialMediaEvaluation] = field(default_factory=list)
    failed_post_keys: list[str] = field(default_factory=list)
    error_messages: dict[str, str] = field(default_factory=dict)
    start_time: datetime = field(default_factory=datetime.now)
    end_time: datetime | None = None
    total_processed: int = 0
    total_succeeded: int = 0
    total_failed: int = 0

    def add_success(self, evaluation: SocialMediaEvaluation) -> None:
        """Add a successful evaluation."""
        self.successful_evaluations.append(evaluation)
        self.total_processed += 1
        self.total_succeeded += 1

    def add_failure(self, post_key: str, error_msg: str) -> None:
        """Add a failed evaluation."""
        self.failed_post_keys.append(post_key)
        self.error_messages[post_key] = error_msg
        self.total_processed += 1
        self.total_failed += 1

    def complete(self) -> None:
        """Mark the batch as complete."""
        self.end_time = datetime.now()

    def get_processing_time(self) -> float:
        """Get total processing time in seconds."""
        if not self.end_time:
            return 0.0
        return (self.end_time - self.start_time).total_seconds()

    def get_success_rate(self) -> float:
        """Get success rate as a percentage."""
        if not self.total_processed:
            return 0.0
        return (self.total_succeeded / self.total_processed) * 100
