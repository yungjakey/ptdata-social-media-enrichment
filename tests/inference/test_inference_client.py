import functools
import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from openai import RateLimitError
from pydantic import BaseModel, Field

from src.inference.client import OpenAIClient

logger = logging.getLogger(__name__)


class InputModel(BaseModel):
    """Test input model."""

    text: str = Field(..., description="Input text")
    score: float = Field(..., description="Input score")


class OutputModel(BaseModel):
    """Test output model."""

    sentiment: str = Field(..., description="Sentiment analysis")
    confidence: float = Field(..., description="Confidence score")


@pytest.fixture
def config():
    """Create test config."""
    return {
        "api_key": "test-key",
        "api_base": "https://test.openai.azure.com",
        "api_version": "2024-10-01-preview",
        "engine": "gpt-4",
    }


@pytest.fixture
def client(config, monkeypatch):
    """Create test client with mocked initialization."""
    # Create a mock client
    mock_client = MagicMock()
    mock_client.chat = MagicMock()
    mock_client.chat.completions = MagicMock()
    mock_client.close = AsyncMock()  # Add async close method

    # Patch the _create_client method to return the mock client
    def mock_create_client(self):
        return mock_client

    client = OpenAIClient(config)
    monkeypatch.setattr(client, "_create_client", functools.partial(mock_create_client, client))

    # Trigger client creation
    client.client

    return client


@pytest.fixture
def mock_completion():
    """Create mock completion."""
    completion = MagicMock()
    completion.choices = [
        MagicMock(message=MagicMock(content='{"sentiment": "positive", "confidence": 0.95}'))
    ]
    return completion


class TestOpenAIClientRecordProcessing:
    """Test suite for OpenAI client record processing methods."""

    @pytest.mark.asyncio
    async def test_process_record_success(self, client, mock_completion):
        """Test successful processing of a single record."""
        messages = [
            {"role": "system", "content": "Test system message"},
            {"role": "user", "content": "Test user message"},
        ]

        mock_completion.choices = [
            MagicMock(
                message=MagicMock(content=json.dumps({"sentiment": "positive", "confidence": 0.95}))
            )
        ]
        client._client.chat.completions.create = AsyncMock(return_value=mock_completion)

        result = await client._process_record(messages, OutputModel)
        assert result.sentiment == "positive"
        assert result.confidence == 0.95

    @pytest.mark.asyncio
    async def test_process_record_error(self, client):
        """Test error handling in record processing."""
        messages = [{"role": "system", "content": "Test"}]

        client._client.chat.completions.create = AsyncMock(side_effect=ValueError("Test error"))

        with pytest.raises(ValueError, match="Test error"):
            await client._process_record(messages, OutputModel)

    @pytest.mark.asyncio
    async def test_process_record_rate_limit(self, client, mock_completion):
        """Test rate limit handling with retries."""
        messages = [{"role": "system", "content": "Test"}]

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "1"}
        mock_response.text = "Rate limit exceeded"
        mock_response.json.return_value = {"error": {"message": "Rate limit exceeded"}}

        client._client.chat.completions.create = AsyncMock(
            side_effect=[
                RateLimitError(
                    "Rate limit exceeded",
                    response=mock_response,
                    body={"error": {"message": "Rate limit exceeded"}},
                ),
                MagicMock(
                    choices=[
                        MagicMock(
                            message=MagicMock(
                                content=json.dumps({"sentiment": "positive", "confidence": 0.95})
                            )
                        )
                    ]
                ),
            ]
        )

        result = await client._process_record(messages, OutputModel)
        assert result.sentiment == "positive"
        assert result.confidence == 0.95


class TestOpenAIClientBatchProcessing:
    """Test suite for OpenAI client batch processing methods."""

    @pytest.mark.asyncio
    async def test_process_batch_success(self, client):
        """Test successful batch processing."""
        records = [
            {"text": "Test 1", "score": 0.5},
            {"text": "Test 2", "score": 0.6},
        ]

        mock_responses = [
            MagicMock(
                choices=[
                    MagicMock(
                        message=MagicMock(
                            content=json.dumps({"sentiment": "positive", "confidence": 0.95})
                        )
                    )
                ]
            ),
            MagicMock(
                choices=[
                    MagicMock(
                        message=MagicMock(
                            content=json.dumps({"sentiment": "positive", "confidence": 0.95})
                        )
                    )
                ]
            ),
        ]

        client._client.chat.completions.create = AsyncMock(side_effect=mock_responses)

        results = await client.process_batch(records, InputModel, OutputModel)
        assert len(results) == 2
        for result in results:
            assert result.sentiment == "positive"
            assert result.confidence == 0.95

    @pytest.mark.asyncio
    async def test_batch_processing_error_handling(self, client):
        """Test batch processing error handling."""
        records = [
            {"text": "Test 1", "score": 0.5},
            {"text": "Test 2", "score": 0.6},
            {"text": "Test 3", "score": 0.7},
        ]

        async def mock_create_side_effect(*, messages, model, **kwargs):
            if "Test 2" in messages[1]["content"]:
                raise ValueError("API Error")
            return MagicMock(
                choices=[
                    MagicMock(
                        message=MagicMock(
                            content=json.dumps({"sentiment": "positive", "confidence": 0.95})
                        )
                    )
                ]
            )

        client._client.chat.completions.create = AsyncMock(side_effect=mock_create_side_effect)

        # Configure the mock to return different responses
        with pytest.raises(ValueError, match="API Error"):
            await client.process_batch(records, InputModel, OutputModel)


class TestOpenAIClientConcurrency:
    """Test suite for OpenAI client concurrency methods."""

    @pytest.mark.asyncio
    async def test_concurrent_processing(self, client, mock_completion):
        """Test concurrent processing with semaphore."""
        records = [{"text": f"Test {i}", "score": 0.5} for i in range(4)]

        # Configure the mock to return a completion for each record
        client._client.chat.completions.create = AsyncMock(
            return_value=MagicMock(
                choices=[
                    MagicMock(
                        message=MagicMock(
                            content=json.dumps({"sentiment": "positive", "confidence": 0.95})
                        )
                    )
                ]
            )
        )

        # Process batch and verify results
        results = await client.process_batch(records, InputModel, OutputModel)
        assert len(results) == len(records)


class TestOpenAIClientLifecycle:
    """Test suite for OpenAI client lifecycle methods."""

    def test_client_initialization(self, config):
        """Test client initialization."""
        client = OpenAIClient(config)
        assert client._client is None

    def test_lazy_client_loading(self, config, monkeypatch):
        """Test lazy loading of client."""
        client = OpenAIClient(config)

        # Mock the _create_client method
        mock_client = MagicMock()

        def mock_create_client():
            return mock_client

        monkeypatch.setattr(client, "_create_client", mock_create_client)

        assert client._client is None
        first_client = client.client
        assert first_client is not None

    @pytest.mark.asyncio
    async def test_client_reset(self, config, monkeypatch):
        """Test client reset mechanism."""
        client = OpenAIClient(config)

        # Mock the _create_client method to return different clients
        def mock_create_client(self):
            mock_client = MagicMock()
            mock_client.close = AsyncMock()  # Add async close method
            return mock_client

        monkeypatch.setattr(client, "_create_client", functools.partial(mock_create_client, client))

        first_client = client.client
        await client.reset()
        second_client = client.client
        assert first_client is not second_client

    @pytest.mark.asyncio
    async def test_context_manager_async(self, config, monkeypatch):
        """Test async context manager."""
        # Mock the _create_client method
        mock_client = MagicMock()
        mock_client.close = AsyncMock()  # Add async close method

        def mock_create_client(self):
            return mock_client

        monkeypatch.setattr(OpenAIClient, "_create_client", mock_create_client)

        async with OpenAIClient(config) as client:
            assert client.client is not None

    def test_context_manager_sync(self, config, monkeypatch):
        """Test sync context manager."""
        # Mock the _create_client method
        mock_client = MagicMock()
        mock_client.close = AsyncMock()  # Add async close method

        def mock_create_client(self):
            return mock_client

        monkeypatch.setattr(OpenAIClient, "_create_client", mock_create_client)

        with OpenAIClient(config) as client:
            assert client.client is not None


class TestOpenAIClientEdgeCases:
    """Test suite for OpenAI client edge cases."""

    @pytest.mark.asyncio
    async def test_process_record_no_choices(self, client):
        """Test handling of completion with no choices."""
        messages = [{"role": "system", "content": "Test"}]

        # Create a mock completion with no choices
        mock_completion = MagicMock()
        mock_completion.choices = []

        # Patch the client's chat.completions.create method
        with patch.object(
            client.client.chat.completions, "create", new_callable=AsyncMock
        ) as mock_create:
            mock_create.return_value = mock_completion

            # Verify that a ValueError is raised when no choices are returned
            with pytest.raises(ValueError, match="No completion choices returned"):
                await client._process_record(messages=messages, response_format=OutputModel)

    @pytest.mark.asyncio
    async def test_process_record_json_decode_error(self, client):
        """Test handling of JSON decoding errors in record processing."""
        messages = [{"role": "system", "content": "Test"}]

        # Mock a completion with invalid JSON content
        mock_completion = MagicMock()
        mock_completion.choices = [MagicMock(message=MagicMock(content="Invalid JSON"))]

        client._client.chat.completions.create = AsyncMock(return_value=mock_completion)

        with pytest.raises(ValueError, match="Response does not match expected format"):
            await client._process_record(messages, OutputModel)

    @pytest.mark.asyncio
    async def test_process_record_validation_error(self, client):
        """Test handling of Pydantic validation errors."""
        messages = [{"role": "system", "content": "Test"}]

        # Mock a completion with partially valid JSON
        mock_completion = MagicMock()
        mock_completion.choices = [MagicMock(message=MagicMock(content='{"invalid_key": "value"}'))]

        client._client.chat.completions.create = AsyncMock(return_value=mock_completion)

        with pytest.raises(ValueError, match="Response does not match expected format"):
            await client._process_record(messages, OutputModel)

    @pytest.mark.asyncio
    async def test_process_batch_partial_validation_error(self, client):
        """Test batch processing with some records failing validation."""
        records = [
            {"text": "Valid 1", "score": 0.5},
            {"invalid": "record"},  # Invalid record
            {"text": "Valid 2", "score": 0.6},
        ]

        mock_responses = [
            MagicMock(
                choices=[
                    MagicMock(
                        message=MagicMock(
                            content=json.dumps({"sentiment": "positive", "confidence": 0.95})
                        )
                    )
                ]
            ),
            MagicMock(
                choices=[
                    MagicMock(
                        message=MagicMock(
                            content=json.dumps({"sentiment": "positive", "confidence": 0.95})
                        )
                    )
                ]
            ),
        ]

        client._client.chat.completions.create = AsyncMock(side_effect=mock_responses)

        results = await client.process_batch(records, InputModel, OutputModel)
        assert len(results) == 2  # Only valid records should be processed

    def test_cleanup_error_handling(self, client):
        """Test cleanup error handling paths."""
        # Simulate an error during context manager usage
        with pytest.raises(Exception, match="Close error"):
            with client:
                client._client = MagicMock()
                raise Exception("Close error")

        # Verify that the client is reset after the context
        assert client._client is None
