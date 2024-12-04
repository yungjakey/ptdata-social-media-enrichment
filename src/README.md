# Social Media Enrichment Service

A type-safe and modular framework for enriching social media data through various providers and data sources.

## Project Structure

```
src/
├── common/           # Core type definitions and utilities
│   ├── types.py     # Base types and singleton components
│   └── README.md    # Common module documentation
│
├── connectors/      # I/O and service connectors
│   ├── aws/        # AWS-specific implementations
│   │   ├── client.py   # AWS connector implementation
│   │   ├── types.py    # AWS-specific types
│   │   └── utils.py    # AWS helper utilities
│   ├── client.py   # Base connector interfaces
│   └── utils.py    # Connector utilities
│
├── inference/       # ML inference services
│   ├── client.py   # Inference client implementation
│   └── types.py    # Inference-specific types
│
└── models/         # Data model definitions
    ├── builder.py  # Dynamic model builder
    └── types.py    # Model-specific types
```

## Design Patterns & Principles

### 1. Type Safety

We enforce type safety through several mechanisms:
- Pydantic models for configuration validation
- Generic type parameters for component specialization
- Runtime type checking through Python's type hints
- Custom validators for domain-specific constraints

Example:
```python
class Config(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)
```

### 2. Singleton Pattern

Configuration management uses the singleton pattern to ensure:
- Single source of truth for component configuration
- Thread-safe configuration access
- Consistent state across component instances

Example:
```python
class Component(Generic[T]):
    _config: ClassVar[T | None] = None
    
    @classmethod
    def get_config(cls) -> T:
        if cls._config is None:
            raise ValueError(f"No configuration set for {cls.__name__}")
        return cls._config
```

### 3. Modularity

The codebase is organized into focused modules:
- `common`: Core types and utilities
- `connectors`: I/O abstractions
- `inference`: ML service integrations
- `models`: Data model definitions

Each module:
- Has a clear, single responsibility
- Defines explicit interfaces
- Is independently testable
- Can be extended without modifying existing code

### 4. Extensibility

The system is designed for easy extension through:
1. **Abstract Base Classes**: Define clear interfaces for new implementations
2. **Generic Types**: Allow type-safe specialization
3. **Factory Methods**: Enable runtime component creation
4. **Configuration Inheritance**: Allow specialized config types

Example adding a new connector:
```python
class NewConnector(Connector[NewConfig]):
    kind = "new"
    
    async def read(self) -> AsyncGenerator[BaseModel]:
        # Implementation
        pass
```

### 5. Single Source of Truth

We maintain single sources of truth through:
1. **Centralized Configuration**: All component configs inherit from base `Config`
2. **Type Hierarchies**: Clear inheritance chains for specialization
3. **Singleton Pattern**: Ensures consistent state
4. **Factory Methods**: Centralized component creation

## Best Practices

1. **Configuration**:
   - Always validate configs through Pydantic models
   - Use strict type checking
   - Define explicit schemas
   - Never duplicate config definitions

2. **Error Handling**:
   - Use custom exception types
   - Provide detailed error messages
   - Handle cleanup in `__aexit__`
   - Log errors appropriately

3. **Resource Management**:
   - Use async context managers
   - Implement proper cleanup
   - Pool and reuse connections
   - Handle timeouts

4. **Testing**:
   - Unit test each component
   - Mock external services
   - Test error conditions
   - Validate type constraints
