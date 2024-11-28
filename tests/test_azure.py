"""Test Azure OpenAI client functionality."""

import asyncio
from typing import ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from openai import APIStatusError, RateLimitError
from openai.types.chat import ChatCompletion, ChatCompletionMessage
from pydantic import BaseModel, Field

from src.azure.client import OpenAIClient
from src.azure.types import APIVersion, Message, ModelName, OpenAIConfig


class MockInputModel(BaseModel):
    """Mock input model for testing."""

    text: str
    language: str = "en"
    primary_keys: list[str] = Field(default_factory=list)

    _BASE_PROMPT: ClassVar[str] = "Analyze text: {text}\nLanguage: {language}"

    @classmethod
    def prompt(cls, **kwargs) -> str:
        """Generate prompt from data."""
        return cls._BASE_PROMPT.format(**kwargs)


class MockOutputModel(BaseModel):
    """Mock output model for testing."""

    sentiment: str
    topics: list[str]
    primary_keys: list[str] = Field(default_factory=list)

    _BASE_PROMPT: ClassVar[str] = (
        "Analyze the sentiment and topics in the following text. Expected output schema: {schema}"
    )

    @classmethod
    def prompt(cls, **kwargs) -> str:
        """Generate system prompt."""
        return cls._BASE_PROMPT.format(**kwargs)


@pytest.fixture
def openai_config():
    """Create OpenAI config for testing."""
    return OpenAIConfig(
        api_key="test-key",
        api_base="https://test.openai.azure.com/",  # Add trailing slash
        api_version=APIVersion.V2024_10_01,
        engine=ModelName.GPT4O_MINI,
        max_workers=2,
    )


@pytest.fixture
def mock_completion():
    """Create mock OpenAI completion."""
    return ChatCompletion(
        id="test-id",
        model="gpt-4",
        object="chat.completion",
        created=1234567890,
        choices=[
            {
                "finish_reason": "stop",
                "index": 0,
                "message": ChatCompletionMessage(
                    role="assistant",
                    content='{"sentiment": "positive", "topics": ["ai", "technology"]}',
                ),
            }
        ],
        usage={"total_tokens": 50, "prompt_tokens": 30, "completion_tokens": 20},
    )


@pytest.fixture
async def openai_client(openai_config, mock_completion):
    """Create OpenAI client with mocked API."""
    with patch("openai.AsyncClient") as mock_client:
        client_instance = mock_client.return_value
        client_instance.chat.completions.create = AsyncMock(return_value=mock_completion)
        client_instance.close = AsyncMock()
        client = OpenAIClient(openai_config)
        yield client
        await client.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_process_single_record(openai_client, mock_completion):
    """Test processing a single record."""
    # Override the mock to return success
    openai_client.client.chat.completions.create = AsyncMock(return_value=mock_completion)

    messages = [Message(role="user", content="Analyze text: AI is amazing\nLanguage: en")]
    result = await openai_client._process_record(messages=messages)

    assert isinstance(result, dict)
    assert result["sentiment"] == "positive"
    assert "ai" in result["topics"]


@pytest.mark.asyncio
async def test_process_batch(openai_client, mock_completion):
    """Test batch processing of records."""
    # Override the mock to return success for both calls
    openai_client.client.chat.completions.create = AsyncMock(return_value=mock_completion)

    records = [
        {"text": "AI is amazing", "language": "en"},
        {"text": "Technology is great", "language": "en"},
    ]
    results = await openai_client.process_batch(records, MockInputModel, MockOutputModel)

    assert len(results) == 2
    for result in results:
        assert isinstance(result, MockOutputModel)
        assert result.sentiment == "positive"
        assert any(topic in ["ai", "technology"] for topic in result.topics)


@pytest.mark.asyncio
async def test_rate_limit_handling(openai_client, mock_completion):
    """Test handling of rate limit errors."""
    # Create a mock response for the rate limit error
    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_response.json.return_value = {"error": {"message": "Rate limit exceeded"}}
    mock_response.text = "Rate limit exceeded"
    mock_response.headers = {"retry-after": "1"}
    rate_limit_error = RateLimitError(
        message="Rate limit exceeded",
        response=mock_response,
        body={"error": {"message": "Rate limit exceeded"}},
    )

    # Create a new mock with side effects
    mock_create = AsyncMock(side_effect=[rate_limit_error, mock_completion])
    openai_client.client.chat.completions.create = mock_create

    messages = [Message(role="user", content="Test text")]
    result = await openai_client._process_record(messages=messages)

    assert isinstance(result, dict)
    assert result["sentiment"] == "positive"
    assert mock_create.call_count == 2


@pytest.mark.asyncio
async def test_invalid_response_handling(openai_client):
    """Test handling of invalid JSON responses."""
    invalid_completion = ChatCompletion(
        id="test-id",
        model="gpt-4",
        object="chat.completion",
        created=1234567890,
        choices=[
            {
                "finish_reason": "stop",
                "index": 0,
                "message": ChatCompletionMessage(
                    role="assistant",
                    content="Invalid JSON response",
                ),
            }
        ],
        usage={"total_tokens": 50, "prompt_tokens": 30, "completion_tokens": 20},
    )

    openai_client.client.chat.completions.create = AsyncMock(return_value=invalid_completion)

    with pytest.raises(ValueError):
        await openai_client._process_record(messages=[Message(role="user", content="Test")])


@pytest.mark.asyncio
async def test_batch_processing_error_handling(openai_client, mock_completion):
    """Test batch processing with mixed success and failures."""
    # Create a mock error response
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.json.return_value = {"error": {"message": "Internal server error"}}
    mock_response.text = "Internal server error"
    mock_response.headers = {}
    api_error = APIStatusError(
        message="Internal server error",
        response=mock_response,
        body={"error": {"message": "Internal server error"}},
    )

    # Create a new mock with side effects
    mock_create = AsyncMock(side_effect=[mock_completion, api_error])
    openai_client.client.chat.completions.create = mock_create

    records = [
        {"text": "Success case", "language": "en"},
        {"text": "Failure case", "language": "en"},
    ]

    results = await openai_client.process_batch(records, MockInputModel, MockOutputModel)

    assert len(results) == 1  # Only successful results are returned
    assert results[0].sentiment == "positive"
    assert mock_create.call_count == 2


def test_config_validation():
    """Test OpenAI config validation."""
    # Test valid config
    config = OpenAIConfig(
        api_key="test-key",
        api_base="https://test.openai.azure.com/",  # Add trailing slash
        api_version=APIVersion.V2024_10_01,
        engine=ModelName.GPT4O_MINI,
        max_workers=2,
    )
    assert config.engine.max_tokens == 8192

    # Test invalid max_workers
    with pytest.raises(ValueError):
        OpenAIConfig(
            api_key="test-key",
            api_base="https://test.openai.azure.com/",
            api_version=APIVersion.V2024_10_01,
            engine=ModelName.GPT4O_MINI,
            max_workers=-1,  # Use negative value to trigger validation error
        )


@pytest.mark.asyncio
async def test_context_manager(openai_config):
    """Test async context manager functionality."""
    async with OpenAIClient(openai_config) as client:
        assert client.client is not None
        assert isinstance(client.semaphore, asyncio.Semaphore)
        assert client.semaphore._value == 2  # Check semaphore value

    assert client.client is None  # Client should be closed
