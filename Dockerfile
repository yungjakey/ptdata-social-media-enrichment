# Build Stage
FROM public.ecr.aws/lambda/python:3.11 AS builder

WORKDIR /var/task

# Install dependencies into a target directory
COPY poetry.lock pyproject.toml ./
RUN pip install poetry && \
    poetry self add poetry-plugin-export && \
    poetry export --without-hashes -f requirements.txt -o requirements.txt && \
    pip install --target ./deps --no-cache-dir -r requirements.txt

# Copy application source code
COPY . .

# Runtime Stage
FROM public.ecr.aws/lambda/python:3.11 AS runner

WORKDIR /var/task

# Copy dependencies from builder
COPY --from=builder /var/task/deps ./site-packages

# Copy application code
COPY --from=builder /var/task/config ./config
COPY --from=builder /var/task/src ./src
COPY --from=builder /var/task/main.py ./main.py
COPY --from=builder /var/task/handler.py ./handler.py

# Set Lambda handler
CMD ["handler.lambda_handler"]
