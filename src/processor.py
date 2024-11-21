"""Batch processor for social media analytics."""
import asyncio
import logging
from typing import Any

from openai import AsyncOpenAI

from src.model import ProcessingResult

logger = logging.getLogger(__name__)


class BatchProcessor:
    """Process social media records in batches."""

    def __init__(
        self,
        api_key: str,
        api_base: str,
        config: dict[str, Any],
        schema: dict[str, Any],
    ) -> None:
        """Initialize batch processor.
        
        Args:
            api_key: OpenAI API key
            api_base: OpenAI API base URL
            config: OpenAI configuration
            schema: Database schema
        """
        self.client = AsyncOpenAI(
            api_key=api_key,
            base_url=api_base
        )
        self.config = config
        self.schema = schema

    async def process_record(self, record: dict[str, Any]) -> ProcessingResult | None:
        """Process a single social media record.
        
        Args:
            record: Social media record to process
            
        Returns:
            ProcessingResult containing facts and dimensions or None if processing failed
        """
        try:
            # Extract content from record
            content = record.get("content", "")
            if not content:
                logger.warning(f"Empty content for record {record.get('id')}")
                return None

            # Generate completion using OpenAI
            response = await self.client.chat.completions.create(
                model=self.config.model,
                messages=[
                    {"role": "system", "content": self.config.system_prompt},
                    {"role": "user", "content": content}
                ],
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens,
            )

            # Extract completion
            completion = response.choices[0].message.content
            if not completion:
                logger.warning(f"Empty completion for record {record.get('id')}")
                return None

            # Parse completion into facts and dimensions
            facts = {
                "id": record["id"],
                "content": content,
                "completion": completion,
                "model": self.config.model,
                "created_at": record.get("created_at"),
            }

            dims = {
                "id": record["id"],
                "source": record.get("source"),
                "author": record.get("author"),
                "language": record.get("language"),
            }

            return ProcessingResult(facts=facts, dims=dims)

        except Exception as e:
            logger.error(f"Failed to process record {record.get('id')}: {e}")
            return None

    async def process_batch(
        self, records: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Process a batch of social media records.
        
        Args:
            records: List of social media records to process
            
        Returns:
            Tuple of (facts, dimensions) for processed records
        """
        tasks = []
        for record in records:
            task = asyncio.create_task(self.process_record(record))
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        facts = []
        dims = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Task failed: {result}")
                continue

            if result:
                facts.append(result.facts)
                dims.append(result.dims)

        return facts, dims
