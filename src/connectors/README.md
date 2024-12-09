# AWS Connectors

This directory contains the AWS integration code for interacting with AWS Glue and Athena services. The main component is the `AWSConnector` class which handles data writing to S3 and table management in Glue.

## AWS Glue Integration

The connector manages Glue tables with the following specifications:

### Table Configuration
- **Table Type**: `EXTERNAL_TABLE` - Tables point to external S3 data
- **File Format**: Parquet
- **SerDe**: Uses Hive Parquet SerDe (`org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe`)
- **Partitioning**: Supports dynamic partitioning with default time-based partitions (year/month/day)

### Schema Handling
- Data columns and partition columns are managed separately in the Glue table definition
- Schema types are automatically converted from Python/Pydantic types to Glue-compatible types
- Partitions are defined in the `PartitionKeys` field of the table definition
- Main data columns are defined in the `StorageDescriptor.Columns` field

### Table Operations
- Tables are automatically created if they don't exist
- Existing tables are updated with the new schema and configuration if they exist
- Both create and update operations use the same table input format for consistency

## AWS Athena Integration

The connector supports querying data through Athena with the following features:

### Query Execution
- Asynchronous query execution using Athena API
- Results are automatically paginated and parsed
- Supports both DDL and DML queries

### Data Types
- Query results are automatically converted from Athena types to Python types
- Complex types (arrays, structs) are supported through the type conversion system

## Usage Example

```python
connector = AWSConnector(config)
await connector.connect()

# Write data with automatic table creation
await connector.write(
    records=data,
    response_format=MyPydanticModel,
    database="my_database",
    table_name="my_table"
)

# Query data
columns, rows = await connector.read()
```

## Important Notes

1. **Partitioning**: 
   - Default partitioning is by year/month/day
   - Custom partition schemas can be provided during write operations
   - Partition values are automatically extracted from the data

2. **Schema Evolution**:
   - Table schemas are updated automatically when new columns are added
   - Existing column types cannot be changed (AWS Glue limitation)
   - Partition columns cannot be modified after table creation

3. **Performance**:
   - Uses Parquet format for optimal query performance
   - Tables are configured for Hive-style partitioning
   - Athena queries are executed with parallel processing enabled
