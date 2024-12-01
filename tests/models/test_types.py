"""Test model builder."""

import pytest
from pydantic import BaseModel

from src.models.types import ModelBuilder


def test_model_builder_basic():
    """Test basic model builder functionality."""
    builder = ModelBuilder("TestModel")
    model = (
        builder.with_field("name", str)
        .with_field("age", int)
        .with_field("active", bool, False)
        .build()
    )

    # Check model properties
    assert model.__name__ == "TestModel"
    assert issubclass(model, BaseModel)

    # Create instance and validate fields
    instance = model(name="test", age=25)
    assert instance.name == "test"
    assert instance.age == 25
    assert instance.active is False  # Default value


def test_model_builder_nested():
    """Test nested model builder."""
    address_builder = ModelBuilder("Address")
    address_model = (
        address_builder.with_field("street", str).with_field("city", str).with_field("zip", str)
    )

    person_builder = ModelBuilder("Person")
    person_model = (
        person_builder.with_field("name", str).with_field("address", address_model).build()
    )

    # Create instance with nested model
    instance = person_model(
        name="John Doe",
        address={"street": "123 Main St", "city": "Anytown", "zip": "12345"},
    )

    assert instance.name == "John Doe"
    assert instance.address.street == "123 Main St"
    assert instance.address.city == "Anytown"
    assert instance.address.zip == "12345"


def test_model_builder_from_schema():
    """Test building model from schema dictionary."""
    schema = {
        "name": str,
        "age": int,
        "scores": {
            "math": float,
            "science": float,
        },
    }

    builder = ModelBuilder("Student")
    model = builder.from_schema(schema).build()

    # Create instance from schema-based model
    instance = model(
        name="Jane Doe",
        age=20,
        scores={"math": 95.5, "science": 88.0},
    )

    assert instance.name == "Jane Doe"
    assert instance.age == 20
    assert instance.scores.math == 95.5
    assert instance.scores.science == 88.0


def test_model_builder_with_prompt():
    """Test model builder with prompt template."""
    prompt = "This is a {name} who is {age} years old"
    builder = ModelBuilder("Person")
    model = builder.with_field("name", str).with_field("age", int).with_prompt(prompt).build()

    assert model.__doc__ == prompt


def test_model_builder_validation():
    """Test model validation."""
    builder = ModelBuilder("TestModel")
    model = builder.with_field("age", int).build()

    # Test invalid type
    with pytest.raises(ValueError):
        model(age="not_a_number")

    # Test missing required field
    with pytest.raises(ValueError):
        model()


def test_model_builder_complex_schema():
    """Test model builder with complex nested schema."""
    schema = {
        "user": {
            "id": int,
            "name": str,
            "contact": {
                "email": str,
                "phone": str,
            },
        },
        "orders": {
            "count": int,
            "items": {
                "product": str,
                "quantity": int,
            },
        },
    }

    builder = ModelBuilder("ComplexModel")
    model = builder.from_schema(schema).build()

    # Create instance with complex nested structure
    instance = model(
        user={
            "id": 1,
            "name": "John Doe",
            "contact": {
                "email": "john@example.com",
                "phone": "123-456-7890",
            },
        },
        orders={
            "count": 2,
            "items": {
                "product": "Widget",
                "quantity": 5,
            },
        },
    )

    assert instance.user.id == 1
    assert instance.user.name == "John Doe"
    assert instance.user.contact.email == "john@example.com"
    assert instance.orders.count == 2
    assert instance.orders.items.product == "Widget"
