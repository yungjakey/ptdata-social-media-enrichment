# AWS Connectors

This module implements AWS data integration using Apache Iceberg for table management and PyArrow for efficient data handling.

## Core Components

### `AWSConnector` (`client.py`)
- Asynchronous AWS integration using aioboto3
- Apache Iceberg table management via pyiceberg
- PyArrow-based data processing
- Concurrent source table reading
- Incremental processing support

### `IcebergConverter` (`utils.py`)
- Converts between Pydantic models and Iceberg schemas
- Handles type mappings (Python → Iceberg → PyArrow)
- Supports partitioning specifications
- Manages schema evolution

## Features

### Incremental Processing
```python
async def read(self, drop: bool = False) -> pa.Table:
    # Get records from source tables
    tables = await self._get_source_records()
    
    # Get already processed records
    processed = await self._get_processed_records()
    
    # Filter each source table
    filtered_tables = []
    for table, source_config in zip(tables, self.config.source.tables):
        # Only keep records where source datetime > target datetime
        # ...
```

### Type Conversion
```python
class IcebergConverter:
    PY_TO_ICEBERG = {
        bool: BooleanType(),
        str: StringType(),
        int: LongType(),
        float: DoubleType(),
        datetime: TimestampType(),
        date: DateType(),
        bytes: BinaryType(),
    }

    PA_TO_PY = {
        pa.int64(): int,
        pa.string(): str,
        pa.float64(): float,
        pa.bool_(): bool,
        pa.timestamp("us"): datetime,
        # ...
    }
```

### Configuration

```yaml
connector:
  region: eu-central-1
  
  source:
    tables:
      - database: source_db
        table: source_table
        index_field: id
        datetime_field: updated_at
    time_filter_hours: 24
    max_records: 1000
    
  target:
    database: target_db
    table: target_table
    index_field: id
    datetime_field: processed_at
    location: s3://bucket/path
    partition_by: 
      - year
      - month
      - day
```

## Usage Example

```python
async with AWSConnector(config) as connector:
    # Read from source tables with incremental processing
    records = await connector.read()
    
    # Write to target table with schema evolution
    await connector.write(
        records=processed_records,
        model=MyPydanticModel
    )
```

## Key Features

### Credential Management
- Uses AWS credential chain
- Supports Lambda execution role
- Local development via AWS SSO

### Table Management
- Automatic table creation
- Schema evolution support
- Partition management
- Location management

### Data Processing
- Concurrent source table reading
- Incremental processing
- PyArrow-based operations
- Efficient joins and filters

### Error Handling
- Table existence checks
- Schema validation
- Type conversion safety
- Detailed logging

## Best Practices

1. **Configuration**
   - Define appropriate time windows
   - Set reasonable record limits
   - Use meaningful partition keys

2. **Performance**
   - Enable concurrent reading
   - Use efficient filtering
   - Optimize partition strategy

3. **Data Quality**
   - Validate schemas
   - Handle type conversions
   - Track processing timestamps

4. **Resource Management**
   - Use async context managers
   - Clean up connections
   - Monitor memory usage
