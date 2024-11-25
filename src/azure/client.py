"""OpenAI client for batch processing social media content."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from openai import AsyncClient
from openai.types.chat import ChatCompletion
from pydantic import BaseModel

from src.azure.config import OpenAIConfig, Payload

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
            base_url=self.config.base_url,
        )
        self.semaphore = asyncio.Semaphore(self.config.max_workers)

    async def _process_record(self, payload: Payload) -> dict[str, Any]:
        """Process single record with rate limiting."""
        async with self.semaphore:
            logger.debug("Processing payload: %s", payload)

            try:
                completion: ChatCompletion = await self.client.chat.completions.create(
                    **payload.__dict__()
                )
                result = json.loads(completion.choices[0].message.content)
                logger.debug("Processed result: %s", json.dumps(result, indent=2))
                return result

            except Exception as e:
                logger.error("Error processing record: %s", str(e))
                raise

    async def process_batch(
        self, records: list[dict[str, Any]], input_model: BaseModel, output_model: BaseModel
    ) -> list[dict[str, type]]:
        """Process batch of records concurrently."""
        logger.info(
            "Starting %d workers to process %d records", self.config.max_workers, len(records)
        )

        tasks = []
        for record in records:
            user = input_model.prompt(**record)
            system = output_model.prompt(**output_model.dict())
            payload = Payload(user=user, system=system, config=self.config.model)
            tasks.append(self._process_record(payload))

        try:
            response = await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error("Batch processing failed: %s", str(e))
            raise

        # Parse results
        successful, failed = [], []
        for result in response:
            if isinstance(result, Exception):
                failed.append(result)
            else:
                successful.append(result)

        logger.info(
            "Batch processing completed. Successful: %d, Failed: %d", len(successful), len(failed)
        )

        if failed:
            failed_ids = [str(err) for err in failed]
            logger.warning("Failed records: %s", ", ".join(failed_ids))

        return successful

    async def __aenter__(self) -> OpenAIClient:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: type | None, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.client.close()

    def __enter__(self) -> OpenAIClient:
        """Sync context manager entry."""
        return self

    def __exit__(self, exc_type: type | None, exc_val: Any, exc_tb: Any) -> None:
        """Sync context manager exit."""
        asyncio.run(self.client.close())
