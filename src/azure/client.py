"""OpenAI client for batch processing social media content."""

from __future__ import annotations

import asyncio
import json
import logging

import tenacity
from openai import AsyncClient, RateLimitError
from openai.types.chat import ChatCompletion
from pydantic import BaseModel

from src.azure.prompts import Prompts
from src.azure.types import Message, OpenAIConfig

logger = logging.getLogger(__name__)


class OpenAIClient:
    """A client for batch processing with Azure OpenAI services.

    This client provides functionality to:
    - Process batches of records concurrently through Azure OpenAI
    - Apply rate limiting using semaphores
    - Handle both sync and async operations
    - Parse and validate responses using Pydantic models

    The client implements both sync and async context managers for proper cleanup.

    Attributes:
        config (OpenAIConfig): Configuration for Azure OpenAI including API keys and model settings
        client (AsyncClient): Async client for Azure OpenAI API calls
        semaphore (asyncio.Semaphore): Rate limiter for concurrent requests
    """

    def __init__(self, config: OpenAIConfig) -> None:
        """Initialize OpenAI client with config."""
        self.config = config
        self.client = AsyncClient(
            api_key=self.config.api_key,
            base_url=self.config.api_base,
        )
        self.semaphore = asyncio.Semaphore(self.config.max_workers)

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(RateLimitError),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        stop=tenacity.stop_after_attempt(5),
        reraise=True,
    )
    async def _process_record(self, messages: list[Message], **kwargs: type) -> dict[str, type]:
        """Process single record with rate limiting."""
        async with self.semaphore:
            logger.debug("Processing messages: %s", messages)

            completion: ChatCompletion = await self.client.chat.completions.create(
                messages=messages,
                model=self.config.engine,
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens,
                top_p=self.config.top_p,
                presence_penalty=self.config.presence_penalty,
                frequency_penalty=self.config.frequency_penalty,
                stop=self.config.stop,
                response_format=self.config.response_format,
                **kwargs,
            )

            if not completion.choices:
                raise ValueError("No completion choices returned")

            result = json.loads(completion.choices[0].message.content)
            logger.debug("Processed result: %s", json.dumps(result, indent=2))
            return result

    async def process_batch(
        self, records: list[dict[str, type]], input_model: BaseModel, output_model: BaseModel
    ) -> list[BaseModel]:
        """Process batch of records concurrently."""
        logger.info(
            "Starting %d workers to process %d records", self.config.max_workers, len(records)
        )
        tasks = []
        for record in records:
            # TODO: this is garbage
            try:
                input_model(**record)
            except Exception as e:
                logger.error("Invalid input record: %s", str(e))
                raise ValueError(f"Invalid input record: {str(e)}") from e

            messages = Prompts.create_messages(input_model, output_model, **record)
            tasks.append(self._process_record(messages=messages))

        try:
            response = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            raise Exception("Batch processing failed: %s", str(e)) from e

        # Parse results
        successful, failed = [], []
        for result in response:
            if isinstance(result, Exception):
                failed.append(result)
            else:
                try:
                    successful.append(output_model(**result))
                except Exception as e:
                    logger.error("Failed to parse result: %s", str(e))
                    failed.append(e)

        logger.info(
            "Batch processing completed. Successful: %d, Failed: %d", len(successful), len(failed)
        )

        if failed:
            failed_ids = [str(err) for err in failed]
            logger.warning("Failed records: %s", ", ".join(failed_ids))

        return successful

    def __enter__(self) -> OpenAIClient:
        """Enter sync context manager."""
        return self

    def __exit__(self, exc_type: type | None, exc: Exception | None, tb: type | None) -> None:
        """Exit sync context manager."""
        if self.client:
            asyncio.run(self.client.close())
            self.client = None

    async def __aenter__(self) -> OpenAIClient:
        """Enter async context manager."""
        return self

    async def __aexit__(
        self, exc_type: type | None, exc: Exception | None, tb: type | None
    ) -> None:
        """Exit async context manager."""
        if self.client:
            await self.client.close()
            self.client = None
