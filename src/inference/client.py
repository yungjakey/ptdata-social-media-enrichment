"""OpenAI client for batch processing social media content."""

from __future__ import annotations

import asyncio
import json
import logging

import tenacity
from openai import AsyncClient, RateLimitError
from openai.types.chat import ChatCompletion
from pydantic import BaseModel, ValidationError

from src.inference.types import OpenAIConfig

logger = logging.getLogger(__name__)


class OpenAIClient:
    """OpenAI client for batch processing social media content."""

    def __init__(self, config: dict[str, type]) -> None:
        """initialize client"""
        self._config = OpenAIConfig.from_dict(**config)
        self._client = None
        self._semaphore = asyncio.Semaphore(self._config.max_workers)

    def _create_client(self) -> AsyncClient:
        """Create OpenAI client."""
        # Remove proxies from kwargs to avoid unexpected argument
        return AsyncClient(api_key=self._config.api_key, base_url=self._config.api_base)

    @property
    def client(self) -> AsyncClient:
        """lazy load client"""
        if self._client is None:
            self._client = self._create_client()
        return self._client

    async def reset(self):
        """reset client"""
        if self._client:
            try:
                await self._client.close()
            except Exception as e:
                logger.error(f"Error closing client: {e}")
            finally:
                # Force a new client to be created on next access
                self._client = None

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(RateLimitError),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        stop=tenacity.stop_after_attempt(5),
        reraise=True,
    )
    async def _process_record(
        self, messages: list[dict[str, str]], response_format: type[BaseModel]
    ) -> BaseModel:
        """Process single record with rate limiting."""
        async with self._semaphore:
            logger.debug("Processing messages: %s", messages)

            try:
                completion: ChatCompletion = await self.client.chat.completions.create(
                    messages=messages,
                    model=self._config.engine,
                    temperature=self._config.temperature,
                    max_tokens=self._config.max_tokens,
                    top_p=self._config.top_p,
                    presence_penalty=self._config.presence_penalty,
                    frequency_penalty=self._config.frequency_penalty,
                    stop=self._config.stop,
                    response_format=response_format,
                )
            except Exception as e:
                logger.error(f"Error during completion: {e}")
                raise

            if not completion.choices:
                raise ValueError("No completion choices returned")

            try:
                result = json.loads(completion.choices[0].message.content)
                logger.debug("Processed result: %s", json.dumps(result, indent=2))
            except json.JSONDecodeError as e:
                raise ValueError(f"Response does not match expected format: {e}") from e

            try:
                return response_format(**result)
            except ValidationError as e:
                raise ValueError(f"Response does not match expected format: {e}") from e

    async def process_batch(
        self,
        records: list[dict],
        inpt: type[BaseModel],
        outpt: type[BaseModel],
    ) -> list[BaseModel]:
        """Process a batch of records."""
        tasks = []
        for record in records:
            try:
                validated_record = inpt(**record)
            except ValidationError as e:
                logger.error(f"{e}: {json.dumps(record)}")
                continue

            sysmsg = outpt.__doc__
            umsg = f"{inpt.__doc__}\n{json.dumps(validated_record.model_dump())}"
            messages = [
                {"role": "system", "content": sysmsg},
                {"role": "user", "content": umsg},
            ]

            tasks.append(self._process_record(messages=messages, response_format=outpt))

        results = []
        try:
            for r in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(r, ValueError):
                    raise r
                if not isinstance(r, Exception):
                    results.append(r)
        except Exception as e:
            logger.error(f"Batch processing error: {e}")
            raise

        return results

    def __enter__(self) -> OpenAIClient:
        """Synchronous context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Synchronous context manager exit, cleanup resources."""
        asyncio.run(self.close())

    async def __aenter__(self) -> OpenAIClient:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit, cleanup resources."""
        await self.close()

    async def close(self):
        """Close client."""
        if self._client:
            try:
                await self._client.close()
            except Exception as e:
                logger.error(f"Error closing client: {e}")
            finally:
                self._client = None
