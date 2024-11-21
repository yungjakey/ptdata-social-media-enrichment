"""Handles OpenAI processing for social media analytics."""

import asyncio
import json
import logging
from collections.abc import Sequence
from typing import Any

from omegaconf import DictConfig
from openai import AzureOpenAI

from src.model import BatchEvaluationResult, SocialMediaEvaluation
from src.model.input import DynamicInput, create_input_model_from_schema
from src.model.output import AiPostDetails, AiPostMetrics

logger = logging.getLogger(__name__)


def _generate_format_string(
    metrics_class: type[AiPostMetrics], details_class: type[AiPostDetails]
) -> str:
    """Generate format string from model class fields."""

    def _get_field_type(field_type: type) -> str:
        if field_type is float:
            return "float (0-1)"
        elif field_type is str:
            return "str"
        elif field_type is list[str]:
            return "[str]"
        elif field_type is (dict[str, Any] | None):
            return "{}"
        else:
            return str(field_type)

    def _get_class_fields(cls: type) -> dict[str, str]:
        fields = {}
        for field_name, field_type in cls.__annotations__.items():
            if field_name in ("id", "post_key", "evaluation_time"):
                continue
            fields[field_name] = _get_field_type(field_type)
        return fields

    format_dict = {
        "metrics": _get_class_fields(metrics_class),
        "details": _get_class_fields(details_class),
    }

    return json.dumps(format_dict, indent=2)


class BatchProcessor:
    """Handles OpenAI processing for social media analytics"""

    def __init__(self, api_key: str, api_base: str, config: DictConfig, schema: dict[str, type]):
        """Initialize OpenAI client with credentials and configuration"""
        self.client = AzureOpenAI(
            api_key=api_key, azure_endpoint=api_base, api_version=str(config.api_version)
        )
        self.engine = str(config.engine)
        self.temperature = float(config.temperature)
        self.max_tokens = int(config.max_tokens)
        self.max_concurrent = int(config.max_concurrent_requests)
        self.batch_size = int(config.batch_size)
        self.semaphore = asyncio.Semaphore(self.max_concurrent)

        # Create input model from schema
        self.InputModel = create_input_model_from_schema(schema)

        # Generate format string for output
        self.format_string = _generate_format_string(AiPostMetrics, AiPostDetails)

        logger.info(
            f"Initialized Processor with engine: {self.engine}, max concurrent: {self.max_concurrent}"
        )

    def _prepare_prompt(self, prompt_template: str, input_data: DynamicInput) -> str:
        """Prepare prompt by formatting template with input data."""
        return prompt_template.format(
            format=self.format_string, input_data=json.dumps(input_data.to_dict(), indent=2)
        )

    async def _process_record(
        self, record: dict[str, Any], prompt_template: str
    ) -> SocialMediaEvaluation:
        """Process a single record using OpenAI API with concurrency control"""
        async with self.semaphore:
            try:
                # Convert record to input model
                input_data = self.InputModel.from_dict(record)
                prompt = self._prepare_prompt(prompt_template, input_data)

                response = await self.client.chat.completions.create(
                    model=self.engine,
                    temperature=self.temperature,
                    max_tokens=self.max_tokens,
                    messages=[{"role": "system", "content": prompt}],
                )

                content = json.loads(response.choices[0].message.content)
                return SocialMediaEvaluation.from_dict(content, record)

            except Exception as e:
                logger.error(
                    f"Failed to process record {record.get('post_key')}: {e}", exc_info=True
                )
                raise

    async def _process_batch(
        self, records: Sequence[dict[str, Any]], prompt_template: str
    ) -> BatchEvaluationResult:
        """Process a batch of records concurrently"""
        tasks = []
        batch_result = BatchEvaluationResult()

        for record in records:
            task = self._process_record(record, prompt_template)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for record, result in zip(records, results, strict=True):
            if isinstance(result, Exception):
                error_msg = str(result)
                logger.error(
                    f"Batch processing error for post {record.get('post_key')}: {error_msg}"
                )
                batch_result.add_failure(record["post_key"], error_msg)
            else:
                batch_result.add_success(result)

        return batch_result

    async def process_records(
        self, records: list[dict[str, Any]], prompts: DictConfig
    ) -> BatchEvaluationResult:
        """Process records using OpenAI API with batching and concurrency"""
        logger.info(f"Processing {len(records)} records with batch size {self.batch_size}")

        all_results = BatchEvaluationResult()

        # Process in batches
        for i in range(0, len(records), self.batch_size):
            batch = records[i : i + self.batch_size]
            batch_results = await self._process_batch(batch, prompts.analyze)

            # Merge batch results
            for eval in batch_results.successful_evaluations:
                all_results.add_success(eval)
            for post_key, error in batch_results.error_messages.items():
                all_results.add_failure(post_key, error)

            logger.info(
                f"Processed batch {i//self.batch_size + 1}, "
                f"success: {len(batch_results.successful_evaluations)}, "
                f"failed: {len(batch_results.failed_post_keys)}"
            )

        all_results.complete()
        logger.info(
            f"Completed processing with "
            f"success rate: {all_results.get_success_rate():.1f}%, "
            f"processing time: {all_results.get_processing_time():.1f}s"
        )

        return all_results
