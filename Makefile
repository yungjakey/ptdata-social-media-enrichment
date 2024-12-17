# Variables
AWS_REGION = eu-central-1
AWS_ACCOUNT = 471112963254
STACK_NAME = ptdata-social-media-enrichment
S3_BUCKET = ptdata-lambda-artifacts
IMAGE_TAG := $(shell git rev-parse --short HEAD) # Short commit SHA

# Display help for each target
help:
	@echo "Available targets:"
	@echo "  help            - Display this help message"
	@echo "  requirements    - Install dependencies and export requirements.txt"
	@echo "  build-and-push  - Build and push the Docker image to ECR"
	@echo "  deploy          - Package and deploy the CloudFormation stack"
	@echo "  all             - Run requirements, build-and-push, and deploy"

.PHONY: help requirements build-and-push deploy all

# Install dependencies and create requirements.txt
requirements:
	poetry install --only main
	poetry export --without-hashes -f requirements.txt -o requirements.txt

# Build the Docker image explicitly for linux/amd64 and push to ECR
build-and-push: requirements
	docker buildx build \
		--output type=image \
		--platform linux/amd64 \
		--provenance=false \
		--sbom=false \
		--push \
		-t $(AWS_ACCOUNT).dkr.ecr.$(AWS_REGION).amazonaws.com/$(STACK_NAME):$(IMAGE_TAG) \
		.

# Deploy AWS Lambda functions
deploy:
	sam deploy \
		--template-file template.yaml \
		--stack-name $(STACK_NAME) \
		--capabilities CAPABILITY_IAM \
		--region $(AWS_REGION) \
		--resolve-image-repos \
		--parameter-overrides ImageTag=$(IMAGE_TAG)

# Run all steps: requirements, build-and-push, deploy
all: build-and-push deploy
