"""OpenAI client for batch processing social media content."""

from __future__ import annotations

import asyncio
import json
import logging

import tenacity
from openai import AsyncClient, RateLimitError
from openai.types.chat import ChatCompletion
from pydantic import BaseModel, ValidationError

from src.inference.types import InferenceConfig

logger = logging.getLogger(__name__)


class InferenceClient:
    """OpenAI client for batch processing social media content."""

    _client: AsyncClient | None = None

    def __init__(self, config: dict[str, type]) -> None:
        """initialize client"""
        self._config = InferenceConfig.from_dict(**config)
        self._semaphore = asyncio.Semaphore(self._config.max_workers)

    @property
    def client(self) -> AsyncClient:
        """lazy load client"""
        if self._client is None:
            self._client = AsyncClient(api_key=self._config.api_key, base_url=self._config.api_base)
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
            except RateLimitError as e:
                raise Exception("Rate limit exceeded") from e
            except Exception as e:
                raise Exception(
                    f"Failed to process messages: {json.dumps(messages, indent=2)}"
                ) from e

            if not completion.choices:
                raise ValueError("No completion choices returned")

            response = completion.choices[0].message.content
            try:
                result = json.loads(response)
                logger.debug(f"Received result: {result}")
            except json.JSONDecodeError as e:
                raise Exception(f"Failed to decode JSON: {response}") from e

            try:
                return response_format(**result)
            except ValidationError as e:
                raise Exception(f"Response does not match {response_format.__name__}") from e

    async def process_batch(
        self,
        records: list[dict],
        input_model: type[BaseModel],
        output_model: type[BaseModel],
    ) -> list[BaseModel]:
        """Process a batch of records."""
        tasks = []
        for record in records:
            try:
                validated_record = input_model(**record)
            except ValidationError as e:
                logger.error(f"{e}: {json.dumps(record)}")
                continue

            sysmsg = output_model.__doc__
            umsg = f"{input_model.__doc__}\n{json.dumps(validated_record.model_dump())}"
            messages = [
                {"role": "system", "content": sysmsg},
                {"role": "user", "content": umsg},
            ]

            tasks.append(self._process_record(messages=messages, response_format=output_model))

        results = []
        try:
            for r in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(r, ValueError):
                    raise r
                if not isinstance(r, Exception):
                    results.append(r)
        except Exception as e:
            raise Exception("Batch processing error") from e

        return results

    def __enter__(self) -> InferenceClient:
        """Synchronous context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Synchronous context manager exit, cleanup resources."""
        asyncio.run(self.close())

    async def __aenter__(self) -> InferenceClient:
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
