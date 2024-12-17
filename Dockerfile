FROM public.ecr.aws/lambda/python:3.11 AS builder

# workdir
WORKDIR /var/task

# install dependencies
COPY . .
RUN pip install poetry && \
    poetry self add poetry-plugin-export && \
    poetry install --only main && \
    poetry export --without-hashes -f requirements.txt -o requirements.txt

# build lambda layer
FROM public.ecr.aws/lambda/python:3.11 AS runner

# workdir
WORKDIR /var/task

# install requirements
COPY --from=builder /var/task/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# copy source
COPY --from=builder /var/task/config ./config
COPY --from=builder /var/task/src ./src
COPY --from=builder /var/task/main.py ./
COPY --from=builder /var/task/handler.py ./

CMD ["handler.lambda_handler"]
