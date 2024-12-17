# Variables
AWS_REGION = eu-central-1
AWS_ACCOUNT = 471112963254
STACK_NAME = ptdata-social-media-enrichment
S3_BUCKET = ptdata-lambda-artifacts

# Display help for each target
help:
	@echo "Available targets:"
	@echo "  help            - Display this help message"
	@echo "  requirements    - Install dependencies and export requirements.txt"
	@echo "  build           - Build the Docker image"
	@echo "  push            - Push the Docker image to ECR"
	@echo "  deploy          - Package and deploy the CloudFormation stack"
	@echo "  all             - Run requirements, build, push, and deploy"

.PHONY: help requirements build push deploy all

# Install dependencies and create requirements.txt
requirements:
	poetry install --only main
	poetry export --without-hashes -f requirements.txt -o requirements.txt

# Build the Docker image using Docker Manifest V2
build: requirements
	docker buildx build --platform linux/amd64 --output type=docker -t $(AWS_ACCOUNT).dkr.ecr.$(AWS_REGION).amazonaws.com/$(STACK_NAME):latest .

# Push the Docker image to ECR
push: build
	docker push $(AWS_ACCOUNT).dkr.ecr.$(AWS_REGION).amazonaws.com/$(STACK_NAME):latest

# Deploy AWS Lambda function
deploy:
	aws cloudformation package --template-file template.yaml --s3-bucket $(S3_BUCKET) --output-template-file packaged.yaml
	aws cloudformation deploy --template-file packaged.yaml --stack-name $(STACK_NAME) --capabilities CAPABILITY_IAM

# Run all steps: requirements, build, push, deploy
all: build push deploy
