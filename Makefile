# Variables
AWS_REGION = eu-central-1
AWS_ACCOUNT = 471112963254
STACK_NAME = ptdata-social-media-enrichment
S3_BUCKET = ptdata-lambda-artifacts

# Display help for each target
help:
	@echo "Available targets:"
	@echo "  help            - Display this help message"

.PHONY: help requirements build push deploy all


# Install dependencies and create poetry.lock
requirements:
	poetry install --only main
	poetry export --without-hashes -f requirements.txt -o requirements.txt

# Build the Docker image
build: requirements
	docker build -t $(AWS_ACCOUNT).dkr.ecr.$(AWS_REGION).amazonaws.com/$(STACK_NAME):latest .

# Deploy the Docker image
push: 
	aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT).dkr.ecr.$(AWS_REGION).amazonaws.com
	docker push $(AWS_ACCOUNT).dkr.ecr.$(AWS_REGION).amazonaws.com/$(STACK_NAME):latest

# Deploy AWS Lambda function
deploy:
	aws cloudformation package --template-file template.yaml --s3-bucket $(S3_BUCKET) --output-template-file packaged.yaml
	aws cloudformation deploy --template-file packaged.yaml --stack-name $(STACK_NAME) --capabilities CAPABILITY_IAM

all: requirements build push deploy