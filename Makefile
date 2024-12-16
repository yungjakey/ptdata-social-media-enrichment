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
	GITHUB_USERNAME=$(shell git config --get remote.origin.url | sed -n 's#.*:\([^/]*\)/.*#\1#p')
	GITHUB_REPOSITORY=$(shell basename -s .git `git config --get remote.origin.url`)
	sed -e "s/{{AWS_ACCOUNT}}/$(AWS_ACCOUNT)/" \
	    -e "s/{{GITHUB_USERNAME}}/$(GITHUB_USERNAME)/" \
	    -e "s/{{GITHUB_REPOSITORY}}/$(GITHUB_REPOSITORY)/" \
	    policy.template > trust-policy.json
	# Delete existing role and policy if they exist
	aws iam detach-role-policy --role-name GitHubActionsRole --policy-arn arn:aws:iam::aws:policy/AdministratorAccess || true
	aws iam delete-role --role-name GitHubActionsRole || true

	# Create new role with updated policy
	aws iam create-role --role-name GitHubActionsRole --assume-role-policy-document file://trust-policy.json
	ROLE_ARN=$(shell aws iam get-role --role-name GitHubActionsRole --query 'Role.Arn' --output text)
	echo "GitHub Actions Role ARN: $$ROLE_ARN"
	aws iam attach-role-policy --role-name GitHubActionsRole --policy-arn arn:aws:iam::aws:policy/AdministratorAccess
	rm trust-policy.json


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
