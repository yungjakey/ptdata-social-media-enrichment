# Variables
AWS_REGION = eu-central-1
AWS_ACCOUNT = 471112963254
STACK_NAME = ptdata-social-media-enrichment
S3_BUCKET = ptdata-lambda-artifacts

# Display help for each target
help:
	@echo "Available targets:"
	@echo "  help            - Display this help message"
	@echo "  poetry-install  - Install dependencies and create poetry.lock"
	@echo "  clean-python    - Clean Python cache files"
	@echo "  build           - Build the Docker image"
	@echo "  deploy          - Deploy the Docker image"
	@echo "  clean           - Clean up local resources"
	@echo "  all             - Full deployment process"
.PHONY: help deploy clean poetry-install clean-python build deploy-lambda

# Install dependencies and create poetry.lock
poetry-install:
	poetry install --only main
	poetry export --without-hashes --format=requirements.txt > requirements.txt

# Clean Python cache files
clean-python:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Build the Docker image
build: clean-python poetry-install
	docker build -t $(AWS_ACCOUNT).dkr.ecr.$(AWS_REGION).amazonaws.com/$(STACK_NAME):latest .

# Deploy the Docker image
deploy: build
	aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT).dkr.ecr.$(AWS_REGION).amazonaws.com
	docker push $(AWS_ACCOUNT).dkr.ecr.$(AWS_REGION).amazonaws.com/$(STACK_NAME):latest

# Clean up local resources
clean: clean-python
	rm -rf .aws-sam/
	rm -f requirements.txt

# Deploy AWS Lambda function
deploy-lambda:
	aws cloudformation package --template-file template.yaml --s3-bucket $(S3_BUCKET) --output-template-file packaged.yaml
	aws cloudformation deploy --template-file packaged.yaml --stack-name $(STACK_NAME) --capabilities CAPABILITY_IAM
all: deploy
