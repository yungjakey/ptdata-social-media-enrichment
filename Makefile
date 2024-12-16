# Variables
AWS_REGION = eu-central-1
AWS_ACCOUNT = 471112963254
STACK_NAME = ptdata-social-media-enrichment

# Build and deploy commands
.PHONY: deploy clean poetry-install

# Install dependencies and create poetry.lock
poetry-install:
	poetry install --only main
	poetry export --without-hashes --format=requirements.txt > requirements.txt

# Deploy using SAM
deploy: poetry-install
	sam build
	sam deploy --stack-name $(STACK_NAME)

# Clean up local resources
clean:
	rm -rf .aws-sam/
	rm -f requirements.txt

# Full deployment process
all: deploy
