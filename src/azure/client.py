"""OpenAI client for batch processing social media content."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from openai import AsyncClient
from openai.types.chat import ChatCompletion

from src.common import OpenAIConfig, Payload

logger = logging.getLogger(__name__)


class OpenAIClient:
    """OpenAI client for batch processing."""

    def __init__(self, api_key: str, api_base: str, max_workers: int = 10, **kwargs):
        """Initialize OpenAI client with config."""
        self.config = OpenAIConfig.get_instance(
            api_key=api_key, base_url=api_base, max_workers=max_workers, **kwargs
        )
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

    async def process_batch(self, prompts: list[dict[str, type]]) -> list[dict[str, type]]:
        """Process batch of records concurrently."""
        logger.info(
            "Starting %d workers to process %d records", self.config.max_workers, len(prompts)
        )

        batch = [Payload(**prompt) for prompt in prompts]

        try:
            tasks = [self._process_record(record) for record in batch]
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
