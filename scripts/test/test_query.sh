#!/bin/bash

# Sample script to run SQL query via AWS CLI

# Check if a query execution ID was passed as an argument
if [ $# -eq 1 ]; then
  EXECUTION_ID="$1"
else
  # Adjusted SQL query to align with config.yaml settings with better formatting
  QUERY="SELECT p.*, r.* \
  FROM prod_gold.fact_social_media_reaction_post r \
  JOIN prod_gold.dim_post_details p ON r.post_key = p.post_key \
  WHERE r.last_update_time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR \
  AND p.last_update_time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR"

  # Execute the query and capture the result
  RESULT=$(aws athena start-query-execution --query-string "$QUERY" --query-execution-context Database=dev_test --result-configuration OutputLocation=s3://orf-tmp/)

  # Log the full response using jq for better readability
  echo "Response from start-query-execution: $(echo $RESULT | jq .)"

  # Check for query execution ID and log it
  EXECUTION_ID=$(echo $RESULT | jq -r '.QueryExecutionId')
  if [ -z "$EXECUTION_ID" ]; then
    echo "Error: No execution ID returned."
    exit 1
  fi
fi

# Log the execution ID
echo "Query Execution ID: $EXECUTION_ID"

# Wait for the query to complete and check the status with a timeout
TIMEOUT=300 # 5 minutes timeout
START_TIME=$(date +%s)
STATUS="RUNNING"
while [ "$STATUS" == "RUNNING" ]; do
  sleep 2
  STATUS=$(aws athena get-query-execution --query-execution-id "$EXECUTION_ID" | jq -r '.QueryExecution.Status.State')
  echo "Query status: $STATUS"
  CURRENT_TIME=$(date +%s)
  ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
  if [ "$ELAPSED_TIME" -ge "$TIMEOUT" ]; then
    echo "Error: Query execution timed out."
    exit 1
  fi
done

# Check if the query succeeded and retrieve results
if [ "$STATUS" == "SUCCEEDED" ]; then
  echo "Query succeeded. Retrieving results..."
  RESULTS=$(aws athena get-query-results --query-execution-id "$EXECUTION_ID")

  # Aggregate information about the results
  RECORD_COUNT=$(echo $RESULTS | jq -r '.ResultSet.Rows | length')
  COLUMN_COUNT=$(echo $RESULTS | jq -r '.ResultSet.ResultSetMetadata.ColumnInfo | length')
  echo "Number of records returned: $RECORD_COUNT"
  echo "Number of columns: $COLUMN_COUNT"

  if [ "$RECORD_COUNT" -eq 0 ]; then
    echo "Query returned no results."
    exit 0
  fi

  if [ "$COLUMN_COUNT" -eq 0 ]; then
    echo "Query returned no columns."
    exit 0
  fi

  # Log the header
  FIRST_RECORD=$(echo $RESULTS | jq -r '.ResultSet.Rows[0] | .Data | .[] | .VarCharValue')
  echo "First record: $FIRST_RECORD" | tr '\n' ', '
else
  echo "Query failed with status: $STATUS"
  exit 1
fi
