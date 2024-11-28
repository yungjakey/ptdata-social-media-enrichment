from typing import Protocol

from pydantic import BaseModel

from src.azure.types import Message, Role


class Prompts(Protocol):
    """Collection of prompts for different tasks."""

    @classmethod
    def create_messages(
        cls, input_model: type[BaseModel], output_model: type[BaseModel], **kwargs
    ) -> list[Message]:
        """Create messages from input and output models.

        Args:
            input_model: Model containing the input prompt template
            output_model: Model containing the system prompt template
            **kwargs: Additional arguments to pass to the prompt templates
        """
        user = input_model.prompt(**kwargs)
        system = output_model.prompt(schema=output_model.model_json_schema())

        return [
            Message(role=Role.USER, content=user),
            Message(role=Role.SYSTEM, content=system),
        ]

    @staticmethod
    def create_analysis_messages(
        expert_description: str, input_label: str, input_data: str, schema: str
    ) -> list[Message]:
        """Create generic analysis messages for various tasks.

        Args:
            expert_description: Description of the expert's role and task
            input_label: Label for the input data (e.g., 'Social media post', 'Input text')
            input_data: The actual data to be analyzed
            schema: JSON schema for the expected response

        Returns:
            A list of Messages for the analysis task
        """
        return [
            Message(
                role=Role.SYSTEM,
                content=f"""
                    {expert_description}

                    Provide clear reasoning and ensure your response \
                    follows the specified format.

                    Your response must be valid JSON with the following structure:
                    {schema}
                    """.strip(),
            ),
            Message(
                role=Role.USER,
                content=f"""{input_label}: {input_data}""".strip(),
            ),
        ]

    @staticmethod
    def social_media_analysis(schema: str, data: str) -> list[Message]:
        """Get social media analysis messages."""
        return Prompts.create_analysis_messages(
            expert_description="""
                You are a social media analytics expert. Your task is to analyze social media posts \
                and provide detailed metrics and insights.

                For metrics, provide scores between 0 and 1. For details, provide clear explanations \
                and insights that help understand the post's performance and impact.
            """,
            input_label="Social media post",
            input_data=data,
            schema=schema,
        )

    @staticmethod
    def classify(schema: str, text: str) -> list[Message]:
        """Get classification messages."""
        return Prompts.create_analysis_messages(
            expert_description="""
                You are a classification expert. Your task is to classify the given input text \
                into predefined categories.
            """,
            input_label="Input text",
            input_data=text,
            schema=schema,
        )
