# Variables
AWS_REGION = eu-central-1
AWS_ACCOUNT = 471112963254
STACK_NAME = ptdata-social-media-enrichment
S3_BUCKET = ptdata-lambda-artifacts

# Build and deploy commands
.PHONY: deploy clean poetry-install clean-python package

# Install dependencies and create poetry.lock
poetry-install:
	poetry install --only main
	poetry export --without-hashes --format=requirements.txt > requirements.txt

# Clean Python cache files
clean-python:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Package the application
package: clean-python poetry-install
	sam build --skip-pull-image
	sam package --region $(AWS_REGION) --s3-bucket $(S3_BUCKET)

# Deploy using SAM
deploy: package
	sam deploy --stack-name $(STACK_NAME) --region $(AWS_REGION) --capabilities CAPABILITY_IAM

# Clean up local resources
clean: clean-python
	rm -rf .aws-sam/
	rm -f requirements.txt

# Full deployment process
all: deploy
