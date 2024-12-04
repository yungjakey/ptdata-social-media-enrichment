# Modular ETL Framework with LLM Enrichment

## Project Overview

This project implements a modular, type-safe ETL (Extract, Transform, Load) framework with machine learning enrichment, featuring a flexible configuration system and extensible connector architecture.

## Architecture Principles

### 1. Configuration Management
- **Singleton Pattern**: Ensures a single, globally accessible configuration instance
- **Type Safety**: Use of protocols and type hints
- **Flexibility**: Easy updates and custom configurations

### 2. Connector Factory
- **Generic Interface**: Uniform connector creation
- **Type-Safe Registration**: Enforces connector implementation contracts
- **Extensibility**: Easy addition of new connector types

### 3. Component Design
Each major component follows a consistent design pattern:
- Singleton configuration
- Context manager support
- Simple, primitive type interfaces
- Modular and extensible architecture

## Module Breakdown

### Common Configuration (`src/common/config.py`)
```python
class SingletonMeta(type):
    """Metaclass for singleton configuration management."""
    def __call__(cls, *args, **kwargs) -> Any:
        # Ensures single instance per configuration class

class BaseSingletonConfig:
    @classmethod
    def create(cls, **kwargs) -> Self:
        """Create or update singleton instance"""
    
    def update(self, **kwargs) -> Self:
        """Update configuration values"""
```

### Connector Factory (`src/connectors/__init__.py`)
```python
class ConnectorFactory:
    @classmethod
    def register(cls, name: str) -> Callable:
        """
        Decorator for registering connector implementations
        
        Args:
            name (str): Unique connector type identifier
        """
    
    @classmethod
    def create(
        cls, 
        connector_type: str, 
        config: Optional[ConfigProtocol] = None
    ) -> ConnectorProtocol:
        """
        Create a connector instance
        
        Args:
            connector_type: Registered connector name
            config: Optional configuration
        
        Returns:
            Instantiated connector
        """
    
    @classmethod
    def list_available(cls) -> List[str]:
        """List all registered connector types"""
```

### Inference Client (`src/inference/client.py`)
```python
class InferenceClient:
    def __init__(self, config: Optional[InferenceConfig] = None):
        """
        Initialize inference client
        
        Args:
            config: Optional inference configuration
        """
    
    @contextmanager
    def load_model(self) -> Generator[Self, None, None]:
        """
        Context manager for model loading
        
        Yields:
            Initialized inference client with loaded model
        
        Handles:
        - Model loading
        - GPU memory management
        - Automatic resource cleanup
        """
    
    def generate(
        self, 
        prompt: str, 
        max_length: int = 100
    ) -> str:
        """
        Generate text using loaded model
        
        Args:
            prompt: Input text
            max_length: Maximum generation length
        
        Returns:
            Generated text
        """
```

### Model Builder (`src/models/builder.py`)
```python
class ModelBuilder:
    def __init__(self, config: Optional[ModelConfig] = None):
        """
        Initialize model builder
        
        Args:
            config: Optional model configuration
        """
    
    def build(self) -> nn.Module:
        """
        Build a configurable neural network model
        
        Returns:
            Configured PyTorch model
        """
```

### Main ETL Process (`src/main.py`)
```python
def extract_data(connector: ConnectorProtocol) -> pd.DataFrame:
    """
    Extract data from source
    
    Args:
        connector: Configured data source connector
    
    Returns:
        Extracted data as DataFrame
    """

def transform_data(
    df: pd.DataFrame, 
    inference_client: InferenceClient
) -> pd.DataFrame:
    """
    Transform and enrich data using LLM
    
    Args:
        df: Input DataFrame
        inference_client: Configured inference client
    
    Returns:
        Enriched DataFrame with LLM insights
    """

def load_data(
    connector: ConnectorProtocol, 
    df: pd.DataFrame
):
    """
    Load transformed data to storage
    
    Args:
        connector: Configured storage connector
        df: DataFrame to be loaded
    """

def main():
    """
    Orchestrate ETL process:
    1. Initialize components
    2. Build model
    3. Extract data
    4. Transform with LLM enrichment
    5. Load results
    """
```

## Design Considerations

### Modularity
- Each component is loosely coupled
- Simple, primitive type interfaces
- Easy to extend and modify

### Type Safety
- Use of Python type hints
- Protocols for interface enforcement
- Generic type variables

### Configuration Management
- Singleton pattern
- Easy updates
- Primitive type configurations
- Default and custom config support

### Resource Management
- Context managers for safe resource handling
- Automatic cleanup
- GPU memory management

### Extensibility
- Connector factory for easy addition of new connectors
- Configurable components
- Minimal interface requirements

## Performance Considerations
- Lazy loading of models
- GPU support with optional fallback
- Minimal overhead in configuration management

## Potential Improvements
- Add comprehensive logging
- Implement more robust error handling
- Create more sophisticated data validation
- Add metrics and monitoring capabilities

## Getting Started
1. Install dependencies
2. Configure environment variables
3. Customize configurations
4. Run main ETL process

## Dependencies
- PyTorch
- Transformers
- Pandas
- Boto3 / Google Cloud Libraries
- (Add specific version requirements)

## Contributing
- Follow PEP 8 style guidelines
- Maintain type hints
- Write comprehensive tests
- Update documentation

## License
[Add your project license]