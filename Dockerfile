FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==3.5.8 kafka-python==2.0.2 psycopg2-binary==2.9.10

WORKDIR /app
COPY app/ ./app/
COPY sql/ ./sql/
