.PHONY: init init-source init-enrichment run test clean help

# Default AWS configuration
WORKGROUP ?= primary
DATABASE ?= dev_gold
PROFILE ?= sandbox
REGION ?= eu-central-1

# Source tables (input data)
SOURCE_FACT_TABLES := fact_social_media_reaction_post
SOURCE_DIM_TABLES := dim_post_details dim_channel dim_channel_group

# Enrichment tables (AI-generated data)
ENRICHED_FACT_TABLES := fact_social_media_ai_metrics
ENRICHED_DIM_TABLES := dim_post_ai_details

# All tables combined
ALL_TABLES := $(SOURCE_FACT_TABLES) $(SOURCE_DIM_TABLES) $(ENRICHED_FACT_TABLES) $(ENRICHED_DIM_TABLES)

help:
	@echo "Available commands:"
	@echo "  make init              - Initialize all database tables in Athena"
	@echo "  make init-source       - Initialize source tables only (facts and dimensions)"
	@echo "  make init-enrichment   - Initialize enrichment tables only (facts and dimensions)"
	@echo "  make run              - Run the social media enrichment processor"
	@echo "  make test             - Run tests"
	@echo "  make clean            - Clean up temporary files"
	@echo "  make help             - Show this help message"
	@echo ""
	@echo "Configuration variables (can be overridden):"
	@echo "  WORKGROUP  = $(WORKGROUP)"
	@echo "  DATABASE   = $(DATABASE)"
	@echo "  PROFILE    = $(PROFILE)"
	@echo "  REGION     = $(REGION)"
	@echo ""
	@echo "Available tables:"
	@echo "  Source Facts:       $(SOURCE_FACT_TABLES)"
	@echo "  Source Dimensions:  $(SOURCE_DIM_TABLES)"
	@echo "  Enriched Facts:     $(ENRICHED_FACT_TABLES)"
	@echo "  Enriched Dims:      $(ENRICHED_DIM_TABLES)"

init:
	@echo "Initializing all tables..."
	@for table in $(ALL_TABLES); do \
		echo "Creating $$table..."; \
		aws athena start-query-execution \
			--query-string file://scripts/tables/$$table.sql \
			--work-group $(WORKGROUP) \
			--query-execution-context Database=$(DATABASE) \
			--profile $(PROFILE) \
			--region $(REGION); \
	done

init-source:
	@echo "Initializing source tables..."
	@for table in $(SOURCE_FACT_TABLES) $(SOURCE_DIM_TABLES); do \
		echo "Creating $$table..."; \
		aws athena start-query-execution \
			--query-string file://scripts/tables/$$table.sql \
			--work-group $(WORKGROUP) \
			--query-execution-context Database=$(DATABASE) \
			--profile $(PROFILE) \
			--region $(REGION); \
	done

init-enrichment:
	@echo "Initializing enrichment tables..."
	@for table in $(ENRICHED_FACT_TABLES) $(ENRICHED_DIM_TABLES); do \
		echo "Creating $$table..."; \
		aws athena start-query-execution \
			--query-string file://scripts/tables/$$table.sql \
			--work-group $(WORKGROUP) \
			--query-execution-context Database=$(DATABASE) \
			--profile $(PROFILE) \
			--region $(REGION); \
	done

run:
	@echo "Running social media enrichment processor..."
	python -m src.main

test:
	@echo "Running tests..."
	python -m pytest tests/

clean:
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name ".pytest_cache" -exec rm -r {} +
	find . -type d -name ".mypy_cache" -exec rm -r {} +
	find . -type d -name "*.egg-info" -exec rm -r {} +
