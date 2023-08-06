FROM tiangolo/uvicorn-gunicorn:python3.10-slim as BASE

LABEL maintainer="Ming Yang <ymviv@qq.com>"

RUN pip install --no-cache-dir poetry==1.4.0

WORKDIR /tmp

COPY poetry.lock pyproject.toml ./
RUN poetry export --output=requirements.txt && \
    poetry export --dev --output=requirements-dev.txt
