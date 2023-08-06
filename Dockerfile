FROM python:3.10-slim as requirements

RUN pip install --no-cache-dir poetry==1.4.0

WORKDIR /tmp

COPY poetry.lock pyproject.toml ./
RUN poetry export --output=requirements.txt && \
    poetry export --dev --output=requirements-dev.txt


FROM tiangolo/uvicorn-gunicorn:python3.10-slim as api

COPY --from=requirements /tmp/requirements.txt /tmp/requirements-dev.txt /tmp/
RUN pip install --no-cache-dir --no-deps -r /tmp/requirements.txt -r /tmp/requirements-dev.txt

COPY . /app

WORKDIR /app

ENV MODULE_NAME=omni_dl.serve
ENV VARIABLE_NAME=app


FROM python:3.10-slim as worker

COPY --from=requirements /tmp/requirements.txt /tmp/requirements-dev.txt /tmp/
RUN pip install --no-cache-dir --no-deps -r /tmp/requirements.txt -r /tmp/requirements-dev.txt

COPY . /app

WORKDIR /app

CMD ["celery", "-A", "omni_dl.serve.worker", "worker", "--beat", "--loglevel", "INFO", "-c", "1"]
