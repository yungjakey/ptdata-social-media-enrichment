FROM public.ecr.aws/lambda/python:3.11 AS builder

WORKDIR /var/task

# copy everything except .dockerignore content
COPY . .

# Install dependencies using Makefile
RUN make poetry-install

# build lambda layer
FROM public.ecr.aws/lambda/python:3.11 AS runner

WORKDIR ${LAMBDA_TASK_ROOT}

# install requirements
COPY --from=builder /var/task/requirements.txt ./requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# copy source
COPY --from=builder /var/task/config ./config
COPY --from=builder /var/task/src ./src
COPY --from=builder /var/task/main.py ./
COPY --from=builder /var/task/handler.py ./

CMD ["handler.lambda_handler"]
