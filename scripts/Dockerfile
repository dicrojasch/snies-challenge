FROM python:3.10-slim

WORKDIR /scripts

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary openpyxl requests

CMD ["python", "/scripts/ingest_data.py"]
