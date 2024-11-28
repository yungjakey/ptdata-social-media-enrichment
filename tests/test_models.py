"""Test model building and validation functionality."""

from typing import ClassVar

import pytest
from pydantic import ValidationError

from src.azure.types import APIVersion, ModelName, OpenAIConfig
from src.models.builder import Builder
from src.models.types import Base


def test_base_model_validation():
    """Test base model validation with primary keys."""
    # Create a simple model with primary keys
    model = Builder("TestModel").with_field("id", str).with_field("name", str).build()

    # Test valid instantiation with primary keys
    instance = model(id="test", name="Test Name", primary_keys=["id"])
    assert instance.id == "test"
    assert instance.name == "Test Name"
    assert instance.primary_keys == ["id"]

    # Test missing primary key field
    with pytest.raises(ValueError, match="Missing primary keys"):
        model(name="Test Name", primary_keys=["id"])

    # Test missing primary_keys argument
    with pytest.raises(ValueError, match="primary_keys is required"):
        model(id="test", name="Test Name")


def test_prompt_generation():
    """Test prompt template and generation."""

    # Create model with custom prompt template
    class CustomModel(Base):
        _BASE_PROMPT: ClassVar[str] = "Analyze text: {text}\nLanguage: {language}"
        text: str
        language: str = "en"

        @classmethod
        def prompt(cls, text: str, language: str = None) -> str:
            return cls._BASE_PROMPT.format(text=text, language=language or cls.language)

    # Test prompt generation
    prompt = CustomModel.prompt(text="Hello world", language="en")
    assert prompt == "Analyze text: Hello world\nLanguage: en"

    # Test prompt override
    prompt = CustomModel.prompt(text="Hello world", language="fr")
    assert prompt == "Analyze text: Hello world\nLanguage: fr"


def test_nested_models():
    """Test building and using nested models."""
    # Create a nested address model
    address_model = Builder("Address").with_field("street", str).with_field("city", str).build()

    # Create a person model with nested address
    person_model = (
        Builder("Person").with_field("name", str).with_field("address", address_model).build()
    )

    # Test instantiation with nested data
    address_data = {"street": "123 Main St", "city": "New York", "primary_keys": ["street"]}
    instance = person_model(
        name="John",
        address=address_data,
        primary_keys=["name"],
    )
    assert instance.name == "John"
    assert instance.address.street == "123 Main St"
    assert instance.address.city == "New York"


def test_model_from_schema():
    """Test creating model from schema dictionary."""
    schema = {
        "name": str,
        "age": int,
        "address": {
            "street": str,
            "city": str,
        },
    }

    model = Builder("SchemaModel").from_schema(schema).build()

    # Create address data with primary keys
    address_data = {
        "street": "456 Oak St",
        "city": "Boston",
        "primary_keys": ["street"],
    }

    instance = model(
        name="Alice",
        age=30,
        address=address_data,
        primary_keys=["name"],
    )
    assert instance.name == "Alice"
    assert instance.age == 30
    assert instance.address.street == "456 Oak St"


def test_openai_config_integration():
    """Test model with OpenAI configuration."""
    config = OpenAIConfig(
        api_key="test-key",
        api_base="https://test.openai.azure.com",
        api_version=APIVersion.V2024_10_01,
        engine=ModelName.GPT4O_MINI,
        max_workers=2,
    )

    # Create model with OpenAI config
    class AIModel(Base):
        _BASE_PROMPT: ClassVar[str] = "Analyze: {text}"
        text: str

    model = Builder("AIModel").with_field("text", str).with_openai_config(config).build()

    instance = model(text="Test text", primary_keys=["text"])
    assert hasattr(model, "_openai_config")
    assert model._openai_config.api_key == "test-key"
    assert model._openai_config.engine == ModelName.GPT4O_MINI


def test_field_validation():
    """Test field type validation."""
    model = Builder("ValidationModel").with_field("age", int).with_field("email", str).build()

    # Test valid data
    instance = model(age=25, email="test@example.com", primary_keys=["email"])
    assert instance.age == 25
    assert instance.email == "test@example.com"

    # Test invalid type
    with pytest.raises(ValidationError):
        model(age="not a number", email="test@example.com", primary_keys=["email"])

    # Test missing required field
    with pytest.raises(ValueError, match="Missing primary keys"):
        model(age=25, primary_keys=["email"])
