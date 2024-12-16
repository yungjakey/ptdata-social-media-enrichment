FROM public.ecr.aws/lambda/python:3.11 AS builder

# workdir
WORKDIR ${LAMBDA_TASK_ROOT}

# install dependencies
COPY . .
RUN pip install poetry && \
    poetry self add poetry-plugin-export && \
    poetry install --only main && \
    poetry export --without-hashes -f requirements.txt -o requirements.txt

# build lambda layer
FROM public.ecr.aws/lambda/python:3.11 AS runner

# workdir
WORKDIR ${LAMBDA_TASK_ROOT}

# install requirements
COPY --from=builder ${LAMBDA_TASK_ROOT}/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# copy source
COPY --from=builder ${LAMBDA_TASK_ROOT}/config ./config
COPY --from=builder ${LAMBDA_TASK_ROOT}/src ./src
COPY --from=builder ${LAMBDA_TASK_ROOT}/main.py ./
COPY --from=builder ${LAMBDA_TASK_ROOT}/handler.py ./

CMD ["handler.lambda_handler"]
