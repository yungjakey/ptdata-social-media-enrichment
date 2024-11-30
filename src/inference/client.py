"""OpenAI client for batch processing social media content."""

from __future__ import annotations

import asyncio
import json
import logging

import tenacity
from openai import AsyncClient, RateLimitError
from openai.types.chat import ChatCompletion
from pydantic import BaseModel

from src.inference.types import OpenAIConfig

logger = logging.getLogger(__name__)


class OpenAIClient:
    """OpenAI client for batch processing social media content."""

    def __init__(self, config: dict[str, type]) -> None:
        """Initialize OpenAI client with config."""
        self.config = OpenAIConfig.from_config(config)
        self.semaphore = asyncio.Semaphore(self.config.max_workers)
        self._client = None

    def connect(self) -> AsyncClient:
        """Establish connection to Azure OpenAI."""
        self._client = AsyncClient(
            api_key=self.config.api_key,
            base_url=self.config.api_base,
        )
        return self._client

    @property
    def client(self) -> AsyncClient:
        """Get OpenAI client instance."""
        if self._client is None:
            self._client = self.connect()
        return self._client

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(RateLimitError),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        stop=tenacity.stop_after_attempt(5),
        reraise=True,
    )
    async def _process_record(
        self, messages: list[dict[str, str]], response_format: BaseModel
    ) -> BaseModel:
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
                response_format=response_format,
            )

            if not completion.choices:
                raise ValueError("No completion choices returned")

            result = json.loads(completion.choices[0].message.content)
            logger.debug("Processed result: %s", json.dumps(result, indent=2))
            return result

    async def process_batch(
        self, records: list[dict[str, type]], inpt: BaseModel, outpt: BaseModel
    ) -> list[BaseModel]:
        """Process batch of records concurrently."""
        logger.info(
            "Starting %d workers to process %d records", self.config.max_workers, len(records)
        )
        tasks = []
        for record in records:
            try:
                record = inpt(**record)
            except Exception as e:
                raise ValueError(f"Invalid input record: {record}") from e

            system = outpt.__doc__
            user = f"{inpt.__doc__}\n{json.dumps(record.model_dump_json())}"
            messages = [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ]
            tasks.append(self._process_record(messages=messages))

        try:
            response = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            raise RuntimeError("Batch processing failed: %s", str(e)) from e

        # Parse results
        successful, failed = [], []
        for result in response:
            if isinstance(result, Exception):
                failed.append(result)
            else:
                try:
                    successful.append(outpt(**result))
                except Exception as e:
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
        if self._client:
            asyncio.run(self._client.close())
            self._client = None

    async def __aenter__(self) -> OpenAIClient:
        """Enter async context manager."""
        return self

    async def __aexit__(
        self, exc_type: type | None, exc: Exception | None, tb: type | None
    ) -> None:
        """Exit async context manager."""
        if self._client:
            await self._client.close()
            self._client = None
