"""OpenAI client for batch processing social media content."""

from __future__ import annotations

import asyncio
import json
import logging

import pyarrow as pa
import tenacity
from openai import AsyncAzureOpenAI, RateLimitError
from openai.types.chat import ChatCompletion

from src.common.component import ComponentFactory
from src.common.utils import ArrowConverter, CustomEncoder

from .config import InferenceConfig

logger = logging.getLogger(__name__)


class InferenceClient(ComponentFactory[InferenceConfig]):
    """Manages inference client configuration."""

    _config_type = InferenceConfig

    def __init__(self, config: InferenceConfig) -> None:
        """Initialize client."""
        super().__init__(config)

        self.model = self.config.response_format
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

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(RateLimitError),
        wait=tenacity.wait_exponential(multiplier=1, min=2, max=30),
        stop=tenacity.stop_after_attempt(5),
        reraise=True,
    )
    async def _process_record(self, record: dict[str, type]) -> dict[str, type] | None:
        """Process single record with rate limiting."""

        async with self._semaphore:
            filtered_record = {
                k: v for k, v in record.items() if k in self.config.include_fields
            }  # and isinstance(v, str | int | float | bool)

            sysmsg = self.model.get_prompt()
            usrmsg = json.dumps(filtered_record, cls=CustomEncoder, ensure_ascii=True)

            messages: list = [
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

            if not content:
                raise ValueError("Empty completion content")

            try:
                return json.loads(content)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON response: {content}") from e

    async def process_batch(self, records: pa.Table) -> pa.Table:
        """Process a batch of records."""
        if not len(records):
            return pa.Table.from_pylist([])

        # Get metadata
        index_col = records.schema.metadata.get(b"index").decode()

        # Create tasks for processing records
        tasks = [self._process_record(record) for record in records.to_pylist()]

        # Process records concurrently
        response = await asyncio.gather(*tasks, return_exceptions=True)

        # Initialize results and mask
        results, mask = [], [False] * len(records)  # Ensure mask length matches records length
        for i, (rec, res) in enumerate(zip(records.to_pylist(), response, strict=False)):
            if isinstance(res, BaseException):
                logger.error(f"Error processing record: {rec}")
                continue  # Leave mask[i] as False
            if not res or not all(res.keys()):
                logger.warning(f"Empty response for record: {rec}")
                continue
            results.append(res)
            mask[i] = True  # Mark the corresponding record as valid

        if not len(results):
            logger.warning("No valid responses found")
            return pa.Table.from_pylist([])

        # Create the resulting table
        schema = ArrowConverter.to_arrow_schema(self.model)
        result_table = pa.Table.from_pylist(results, schema=schema).append_column(
            index_col, records.filter(pa.array(mask)).column(index_col)
        )

        logger.debug(f"Result table: {result_table.schema}")
        metadata = {"index": index_col}

        return result_table.replace_schema_metadata(metadata)

    async def __aenter__(self) -> InferenceClient:
        """Async context manager entry."""

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit, cleanup resources."""

        # Wait for all pending tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.close()
