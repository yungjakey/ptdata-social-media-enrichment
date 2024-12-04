"""AWS package for social media analytics."""

from .client import AWSConfig
from .client import AWSConnector as AWSClient

__all__ = ["AWSClient", "AWSConfig"]
