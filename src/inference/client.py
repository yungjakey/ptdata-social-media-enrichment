"""OpenAI client for batch processing social media content."""

from __future__ import annotations

import asyncio
import json
import logging
import os

import pyarrow as pa
import tenacity
from openai import AsyncAzureOpenAI, RateLimitError
from openai.types.chat import ChatCompletion
from pydantic import BaseModel

from src.common import DateTimeEncoder
from src.common.component import ComponentFactory

from .config import InferenceConfig

logger = logging.getLogger(__name__)


class InferenceClient(ComponentFactory):
    """Manages inference client configuration."""

    def __init__(self, config: InferenceConfig) -> None:
        """Initialize client."""
        super().__init__(config)

        if not config.api_key or not config.api_base:
            raise ValueError("OPENAI_API_KEY and OPENAI_API_BASE must be set")

        self.model: type[BaseModel] = self.config.response_format
        self._client: AsyncAzureOpenAI | None = None
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(self.config.workers)
        self._tasks: set[asyncio.Task] = set()  # Track active tasks

        logger.debug(f"Inference client initialized with config: {config}")

    _config = InferenceConfig

    @classmethod
    def from_config(cls, config: dict[str, type]) -> InferenceClient:
        """Create client from config."""
        if not config.get("api_key"):
            config["api_key"] = os.getenv("OPENAI_API_KEY")
        if not config.get("api_base"):
            config["api_base"] = os.getenv("OPENAI_API_BASE")

        return super().from_config(config)

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

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(RateLimitError),
        wait=tenacity.wait_exponential(multiplier=1, min=2, max=30),
        stop=tenacity.stop_after_attempt(5),
        reraise=True,
    )
    async def _process_record(self, record: dict[str, type], index: str) -> dict[str, type]:
        """Process single record with rate limiting."""
        async with self._semaphore:
            filtered_record = {
                k: v for k, v in record.items() if k not in self.config.exclude_fields
            }

            sysmsg = self.model.get_prompt()
            usrmsg = json.dumps(filtered_record, indent=2, cls=DateTimeEncoder, ensure_ascii=True)

            messages = [
                {"role": "system", "content": sysmsg},
                {"role": "user", "content": usrmsg},
            ]

            logger.debug(
                f"Processing messages: {json.dumps(messages, indent=2, cls=DateTimeEncoder)}"
            )
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
                result = json.loads(content)  # Parse JSON response
                result[index] = record[index]
                return result
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON response: {content}") from e

    async def process_batch(self, records: pa.Table, index: str) -> pa.Table:
        """Process a batch of records."""
        if not len(records):
            return pa.Table.from_pylist([])

        # Create tasks for valid records
        tasks = []
        for record in records.to_pylist():
            tasks.append(self._process_record(record, index))

        # Process records concurrently
        response = await asyncio.gather(*tasks, return_exceptions=True)
        results = []
        for r in response:
            if isinstance(r, Exception):
                logger.error(f"Error processing record: {r}")
            if not isinstance(r, Exception):
                results.append(r)

        # Create table and cast index field to match source schema
        result_table = pa.Table.from_pylist(results)
        if index:
            # Get the field type from source records
            source_field_type = records.schema.field(index).type
            result_table = result_table.set_column(
                result_table.schema.get_field_index(index),
                index,
                result_table[index].cast(source_field_type),
            )

        return result_table

    async def __aenter__(self) -> InferenceClient:
        """Async context manager entry."""

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit, cleanup resources."""
        # Wait for all pending tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.close()
