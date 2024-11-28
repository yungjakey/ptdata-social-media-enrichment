.PHONY: run interactive install config ddl execute-ddl

# Default config path
CONFIG_PATH ?= config/default.yaml

# Python settings
PYTHON = python3
VENV = .venv
BIN = $(VENV)/bin

# Go settings
GO = go
CLI_DIR = cmd/cli

# Install dependencies
install:
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install -r requirements.txt
	cd $(CLI_DIR) && $(GO) mod tidy

# Run interactive CLI
interactive:
	cd $(CLI_DIR) && $(GO) run .

# Run with specified config
run:
	cd $(CLI_DIR) && $(GO) run . --config $(CONFIG_PATH) --run

# Create DDL for a table
ddl:
ifdef TABLE
	cd $(CLI_DIR) && $(GO) run . --config $(CONFIG_PATH) --create-ddl $(TABLE)
else
	@echo "Usage: make ddl TABLE=<table_name>"
	@exit 1
endif

# Execute DDL file
execute-ddl:
ifdef FILE
	cd $(CLI_DIR) && $(GO) run . --config $(CONFIG_PATH) --execute-ddl $(FILE)
else
	@echo "Usage: make execute-ddl FILE=<path_to_ddl_file>"
	@exit 1
endif

# Create new config from template
config:
	cp $(CONFIG_PATH) config/local.yaml