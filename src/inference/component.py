"""OpenAI client for batch processing social media content."""

from __future__ import annotations

import asyncio
import json
import logging

import tenacity
from openai import AsyncClient, RateLimitError
from openai.types.chat import ChatCompletion
from pydantic import BaseModel, ValidationError

from src.common.component import ComponentFactory
from src.inference.config import InferenceConfig

logger = logging.getLogger(__name__)


class InferenceClient(ComponentFactory):
    """Manages inference client configuration."""

    _config = InferenceConfig

    def __init__(self, config: InferenceConfig) -> None:
        """Initialize client."""
        super().__init__(config)
        self._client: AsyncClient | None = None
        self._semaphore = asyncio.Semaphore(self._config.max_workers)

    @property
    def client(self) -> AsyncClient:
        """Lazy load client."""

        if self._client is None:
            self._client = AsyncClient(api_key=self._config.api_key, base_url=self._config.api_base)
        return self._client

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
        records: list[BaseModel],
        input_model: type[BaseModel],
        output_model: type[BaseModel],
    ) -> list[BaseModel]:
        """Process a batch of records."""

        tasks = []
        for record in records:
            sysmsg = output_model.__doc__
            usrmsg = f"{input_model.__doc__}\n{json.dumps(record.model_dump())}"

            messages = [
                {"role": "system", "content": sysmsg},
                {"role": "user", "content": usrmsg},
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
            raise RuntimeError("Batch processing error") from e

        return results

    async def close(self):
        """Close client."""

        if self._client:
            try:
                await self._client.close()
            except Exception as e:
                logger.error(f"Error closing client: {e}")
            finally:
                self._client = None

    async def reset(self):
        """Reset client and clear resources."""

        if self._client:
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
