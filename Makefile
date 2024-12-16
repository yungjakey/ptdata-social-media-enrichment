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
	@echo "  setup           - Create ECR repository and S3 bucket"
.PHONY: help deploy clean poetry-install clean-python build deploy-lambda setup

# Create ECR repository and S3 bucket
setup:
	aws ecr create-repository --repository-name $(STACK_NAME) || true
	aws s3api create-bucket --bucket $(S3_BUCKET) --region $(AWS_REGION) --create-bucket-configuration LocationConstraint=$(AWS_REGION) || true


# Clean up local resources
clean: clean-python
	rm -rf .aws-sam/
	rm -f requirements.txt
	rm -f packaged.yaml

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

all: requirements build pushdeploy
