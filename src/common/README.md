# Common Components

This directory contains the core shared components and utilities used throughout the project.

## Overview

The common module provides foundational classes and utilities that support the modular architecture of the system:

### Key Components

#### `ComponentFactory` (`component.py`)
- Base class for all configurable components in the system
- Implements the Factory pattern for component creation
- Provides configuration management and validation
- Used by both Connector and Inference modules

#### `BaseConfig` (`config.py`)
- Base configuration class using Pydantic
- Provides type safety and validation for all configurations
- Supports environment variable interpolation
- Includes common configuration patterns

#### Generic Type Definitions
- `TConf`: Generic type for configuration objects
- Type hints for improved code safety and IDE support

## Usage

### Creating a New Component

```python
from src.common.component import ComponentFactory
from src.common.config import BaseConfig

class MyConfig(BaseConfig):
    field1: str
    field2: int

class MyComponent(ComponentFactory):
    _config = MyConfig

    def __init__(self, config: MyConfig) -> None:
        super().__init__(config)
        # Additional initialization
```

### Configuration Management

```python
# Configuration can be loaded from dict
config_dict = {
    "field1": "value1",
    "field2": 42
}

# Create component instance
component = MyComponent.from_config(config_dict)
```

## Best Practices

1. **Type Safety**
   - Always use type hints
   - Extend BaseConfig for new configurations
   - Use Pydantic validators when needed

2. **Component Creation**
   - Use the factory pattern via ComponentFactory
   - Initialize components through from_config()
   - Validate configurations at creation time

3. **Error Handling**
   - Use custom exceptions for specific error cases
   - Provide clear error messages
   - Handle configuration errors gracefully
