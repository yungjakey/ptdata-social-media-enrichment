"""OpenAI client for batch processing social media content."""

from __future__ import annotations

import asyncio
import json
import logging

import tenacity
from openai import AsyncClient, RateLimitError
from openai.types.chat import ChatCompletion
from pydantic import BaseModel

from src.common.component import ComponentFactory
from src.inference.config import InferenceConfig

logger = logging.getLogger(__name__)


class InferenceClient(ComponentFactory):
    """Manages inference client configuration."""

    _config = InferenceConfig

    def __init__(self, config: InferenceConfig) -> None:
        """Initialize client."""
        super().__init__(config)
        self.model: type[BaseModel] = self.config.response_format
        self._client: AsyncClient | None = None
        self._semaphore = asyncio.Semaphore(self.config.workers)

    @property
    def client(self) -> AsyncClient:
        """Lazy load client."""

        if self._client is None:
            self._client = AsyncClient(api_key=self.config.api_key, base_url=self.config.api_base)
        return self._client

    async def close(self):
        """Close client."""

        if not self._client:
            return

        try:
            await self._client.close()
        except Exception as e:
            logger.error(f"Error closing client: {e}")
        finally:
            self._client = None

    async def __aenter__(self) -> InferenceClient:
        """Async context manager entry."""

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit, cleanup resources."""

        await self.close()

    async def _process_record(self, record: dict[str, type]) -> dict[str, type]:
        """Process single record with rate limiting."""

        async with self._semaphore:
            sysmsg = self.model.__doc__
            usrmsg = json.dumps(record.model_dump())
            messages = [
                {"role": "system", "content": sysmsg},
                {"role": "user", "content": usrmsg},
            ]

            logger.debug(f"Processing messages: {json.dumps(messages, indent=2)}")
            try:
                completion: ChatCompletion = await self.client.chat.completions.create(
                    messages=messages,
                    response_format=self.model,
                    model=self.config.engine,
                    temperature=self.config.temperature,
                    max_tokens=self.config.max_tokens,
                    n=1,
                )
            except Exception as e:
                raise Exception(
                    f"Failed to process messages: {json.dumps(messages, indent=2)}"
                ) from e

            if not completion.choices:
                raise ValueError("No completion choices returned")

            response = completion.choices[0].message.content
            try:
                return json.loads(response)
            except json.JSONDecodeError as e:
                raise Exception(f"Failed to decode JSON: {response}") from e

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(RateLimitError),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        stop=tenacity.stop_after_attempt(5),
        reraise=True,
    )
    async def process_batch(self, records: list[dict[str, type]]) -> list[dict[str, type]]:
        """Process a batch of records."""

        tasks = []
        for record in records:
            tasks.append(self._process_record(record))

        results = []
        for r in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(r, Exception):
                logger.error(f"Error processing record: {r}")
                continue
            if not isinstance(r, Exception):
                results.append(r)

        return results
