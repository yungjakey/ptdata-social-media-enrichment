"""Test inference types."""

import pytest
from pydantic import ValidationError

from src.inference.types import APIVersion, ModelName, OpenAIConfig


def test_api_version_enum():
    """Test APIVersion enum."""
    assert APIVersion.V2024_10_01 == "2024-10-01-preview"
    assert APIVersion("2024-10-01-preview") == APIVersion.V2024_10_01

    # Test invalid version
    with pytest.raises(ValueError):
        APIVersion("invalid-version")


def test_model_name_enum():
    """Test ModelName enum."""
    assert ModelName.GPT4 == "gpt-4"
    assert ModelName.GPT4O_MINI == "gpt-4o-mini"
    assert ModelName("gpt-4") == ModelName.GPT4

    # Test max tokens property
    assert ModelName.GPT4.max_tokens == 8192
    assert ModelName.GPT4O_MINI.max_tokens == 8192

    # Test invalid model
    with pytest.raises(ValueError):
        ModelName("invalid-model")


def test_openai_config_basic():
    """Test basic OpenAIConfig creation."""
    config = OpenAIConfig(
        api_key="test-key",
        api_base="https://test.openai.azure.com",
        engine="gpt-4",
    )

    assert config.type == "azure"
    assert config.api_key == "test-key"
    assert config.api_base == "https://test.openai.azure.com"
    assert config.engine == ModelName.GPT4
    assert config.api_version == APIVersion.V2024_10_01  # default
    assert config.max_workers == 5  # default
    assert config.temperature == 0.7  # default
    assert config.max_tokens is None  # default
    assert config.top_p == 1.0  # default
    assert config.presence_penalty == 0.0  # default
    assert config.frequency_penalty == 0.0  # default
    assert config.stop is None  # default
    assert config.response_format is None  # default
    assert config.timeout == 10  # default


def test_openai_config_validation():
    """Test OpenAIConfig validation."""
    # Test empty API key
    with pytest.raises(ValueError, match="api_key cannot be empty"):
        OpenAIConfig(
            api_key="",
            api_base="https://test.openai.azure.com",
            engine="gpt-4",
        )

    # Test empty API base
    with pytest.raises(ValueError, match="api_base cannot be empty"):
        OpenAIConfig(
            api_key="test-key",
            api_base="",
            engine="gpt-4",
        )

    # Test invalid max workers
    with pytest.raises(ValidationError):
        OpenAIConfig(
            api_key="test-key",
            api_base="https://test.openai.azure.com",
            engine="gpt-4",
            max_workers=0,
        )

    # Test invalid temperature
    with pytest.raises(ValidationError):
        OpenAIConfig(
            api_key="test-key",
            api_base="https://test.openai.azure.com",
            engine="gpt-4",
            temperature=2.0,
        )

    # Test invalid presence penalty
    with pytest.raises(ValidationError):
        OpenAIConfig(
            api_key="test-key",
            api_base="https://test.openai.azure.com",
            engine="gpt-4",
            presence_penalty=3.0,
        )


def test_openai_config_from_dict():
    """Test OpenAIConfig.from_dict method."""
    # Test with minimal parameters
    config = OpenAIConfig.from_dict(
        api_key="test-key",
        api_base="https://test.openai.azure.com",
        api_version="2024-10-01-preview",
        engine="gpt-4",
    )
    assert config.api_key == "test-key"
    assert config.api_base == "https://test.openai.azure.com"
    assert config.api_version == APIVersion.V2024_10_01
    assert config.engine == ModelName.GPT4

    # Test with additional parameters
    config = OpenAIConfig.from_dict(
        api_key="test-key",
        api_base="https://test.openai.azure.com",
        api_version="2024-10-01-preview",
        engine="gpt-4",
        max_workers=10,
        temperature=0.5,
        max_tokens=100,
        stop=["END"],
    )
    assert config.max_workers == 10
    assert config.temperature == 0.5
    assert config.max_tokens == 100
    assert config.stop == ["END"]

    # Test invalid API version
    with pytest.raises(ValueError, match="Invalid configuration"):
        OpenAIConfig.from_dict(
            api_key="test-key",
            api_base="https://test.openai.azure.com",
            api_version="invalid-version",
            engine="gpt-4",
        )

    # Test invalid engine
    with pytest.raises(ValueError, match="Invalid configuration"):
        OpenAIConfig.from_dict(
            api_key="test-key",
            api_base="https://test.openai.azure.com",
            api_version="2024-10-01-preview",
            engine="invalid-engine",
        )
