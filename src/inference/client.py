"""OpenAI client for batch processing social media content."""

from __future__ import annotations

import asyncio
import json
import logging

import tenacity
from openai import AsyncAzureOpenAI, RateLimitError
from openai.types.chat import ChatCompletion
from pydantic import BaseModel

from src.common.component import ComponentFactory

from .config import InferenceConfig

logger = logging.getLogger(__name__)


class InferenceClient(ComponentFactory):
    """Manages inference client configuration."""

    _config = InferenceConfig

    def __init__(self, config: InferenceConfig) -> None:
        """Initialize client."""
        super().__init__(config)
        self.model: type[BaseModel] = self.config.response_format
        self._client: AsyncAzureOpenAI | None = None
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(self.config.workers)
        self._tasks: set[asyncio.Task] = set()  # Track active tasks

        logger.debug(f"Inference client initialized with config: {config}")

    @property
    def client(self) -> AsyncAzureOpenAI:
        """Get client."""
        if not self._client:
            self._client = AsyncAzureOpenAI(
                azure_endpoint=self.config.api_base,
                azure_deployment=self.config.deployment,
                api_version=self.config.version,
                api_key=self.config.api_key,
            )
        return self._client

    async def close(self):
        """Close client."""

        try:
            if self._client:
                await self._client.close()
                self._client = None
        except Exception as e:
            logger.debug(f"Error closing client: {e}")

    async def __aenter__(self) -> InferenceClient:
        """Async context manager entry."""

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit, cleanup resources."""
        # Wait for all pending tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.close()

    async def _process_record(self, record: dict[str, type]) -> dict[str, type]:
        """Process single record with rate limiting."""

        async with self._semaphore:
            sysmsg = self.model.get_prompt()
            usrmsg = json.dumps(record)

            messages = [
                {"role": "system", "content": sysmsg},
                {"role": "user", "content": usrmsg},
            ]

            logger.debug(f"Processing messages: {json.dumps(messages, indent=2)}")
            completion: ChatCompletion = await self.client.beta.chat.completions.parse(
                messages=messages,
                response_format=self.model,
                model=self.config.engine,
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens,
                n=1,
            )

            logger.debug(f"Completion object: {completion}")

            if ((choice := completion.choices) is None) or (len(choice) == 0):
                raise ValueError("No completion choices returned")

            content = choice[0].message.content
            logger.debug(f"Completion content: {content}")

            try:
                return json.loads(content)  # Parse JSON response
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON response: {content}") from e

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(RateLimitError),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        stop=tenacity.stop_after_attempt(3),
        reraise=True,
    )
    async def process_batch(self, records: list[dict[str, type]]) -> list[dict[str, type]]:
        """Process a batch of records."""

        tasks = []
        for record in records:
            if not isinstance(record, dict):
                logger.warning(f"Skipping non-dict record: {record}")
                continue
            tasks.append(self._process_record(record))

        if not tasks:
            logger.warning("No valid records to process")
            return []

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        results = []
        for i, o in zip(records, responses, strict=False):
            if isinstance(o, Exception):
                logger.error(f"Error processing record: {str(o)}")
                continue
            if o is None:
                logger.warning("Null record returned")
                continue

            r = {**i, **o}
            results.append(r)
            logger.info(f"Processing record: {json.dumps(r, indent=2)}")

        return results
