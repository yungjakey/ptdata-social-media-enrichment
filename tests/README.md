# Test Suite Design

This document outlines the design principles and practices used in our test suite.

## Overview

The test suite is designed to ensure the reliability and correctness of our social media enrichment pipeline. It covers:
- AWS interactions (Athena and Glue)
- Azure OpenAI integrations
- Main application workflow
- Configuration handling
- Error scenarios and edge cases

## Test Structure

### Directory Organization
```
tests/
├── __init__.py
├── test_aws.py      # AWS client and type tests
├── test_azure.py    # Azure OpenAI client tests
├── test_models.py   # Model building and validation tests
└── test_main.py     # Main application tests
```

### Test Categories

1. **Unit Tests**
   - Focus on testing individual components in isolation
   - Heavy use of mocking for external dependencies
   - Example: `test_aws.py::test_execute_query`

2. **Integration Tests**
   - Test interaction between components
   - Use real configurations but mock external services
   - Example: `test_main.py::test_main_workflow`

3. **Configuration Tests**
   - Validate configuration parsing and validation
   - Test error handling for invalid configs
   - Example: `test_aws.py::test_aws_config_validation`

## Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_aws.py

# Run with coverage
pytest --cov=src --cov-report=term-missing

# Run specific test
pytest tests/test_aws.py::test_execute_query
```

## Testing Practices

### Mocking Strategy

1. **Service Mocks**
   ```python
   @pytest.fixture
   def mock_athena():
       mock = MagicMock()
       mock.start_query_execution.return_value = {"QueryExecutionId": "test-id"}
       return mock
   ```
   - Used for external services (AWS, Azure)
   - Defined as fixtures for reuse
   - Include common response patterns

2. **Configuration Mocks**
   ```python
   @pytest.fixture
   def test_config():
       return {
           "aws": {"region": "eu-central-1"},
           "openai": {"engine": "gpt-4"}
       }
   ```
   - Simulate configuration files
   - Include all required fields
   - Test both valid and invalid configs

3. **Context Mocks**
   ```python
   with patch("sys.argv"), patch("src.main.Path.exists"):
       # Test code here
   ```
   - Mock system context (files, env vars)
   - Use context managers for cleanup
   - Ensure isolation between tests

### Error Handling

1. **Expected Errors**
   ```python
   with pytest.raises(FileNotFoundError) as exc_info:
       function_under_test()
   assert "Expected error message" in str(exc_info.value)
   ```
   - Test specific error types
   - Validate error messages
   - Ensure proper error propagation

2. **Service Errors**
   ```python
   mock_client.side_effect = ClientError(
       {"Error": {"Code": "InvalidRequestException", "Message": "Test error"}},
       "Operation"
   )
   ```
   - Simulate service-specific errors
   - Test retry mechanisms
   - Validate error handling paths

### Async Testing

1. **Async Functions**
   ```python
   @pytest.mark.asyncio
   async def test_async_function():
       result = await function_under_test()
       assert result
   ```
   - Use pytest-asyncio for async tests
   - Handle coroutines properly
   - Test async error scenarios

2. **Mock Async Calls**
   ```python
   mock_client.return_value = AsyncMock(
       return_value={"result": "success"}
   )
   ```
   - Use AsyncMock for async functions
   - Handle async context managers
   - Test async timeouts

### Test Data

1. **Mock Models**
   ```python
   class MockInputModel(BaseModel):
       text: str
       user: str
   ```
   - Use Pydantic models for validation
   - Keep test data minimal but sufficient
   - Avoid test data duplication

2. **Fixtures**
   ```python
   @pytest.fixture
   def test_records():
       return [
           {"id": 1, "text": "Test"},
           {"id": 2, "text": "Another test"}
       ]
   ```
   - Share common test data
   - Use parametrize for variations
   - Include edge cases


## Future Improvements

1. **Property-Based Testing**
   - Add hypothesis for property testing
   - Test with generated data
   - Find edge cases automatically

2. **Performance Testing**
   - Add benchmarks for critical paths
   - Test with large datasets
   - Monitor memory usage

3. **Integration Testing**
   - Add end-to-end tests
   - Test real service integration
   - Add load testing
