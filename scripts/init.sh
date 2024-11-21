#!/bin/bash

# Default values
DATABASE="dev_gold"
BUCKET_PATH="s3://aws-orf-social-media-analytics/dev/gold"
WORKGROUP="primary"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --database)
      DATABASE="$2"
      shift 2
      ;;
    --bucket-path)
      BUCKET_PATH="$2"
      shift 2
      ;;
    --workgroup)
      WORKGROUP="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Read SQL template and replace variables
SQL=$(cat "$(dirname "$0")/init.sql" | \
  sed "s|\${database}|$DATABASE|g" | \
  sed "s|\${bucket_path}|$BUCKET_PATH|g")

# Execute SQL using AWS CLI
aws athena start-query-execution \
  --query-string "$SQL" \
  --work-group "$WORKGROUP" \
  --query-execution-context "Database=$DATABASE"
