FROM python:3.10-slim

WORKDIR /app

# Update package lists and install build and network tools
# Update the RUN command to include gawk
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    libxml2-utils \
    findutils \
    curl \
    tidy \
    ca-certificates \
    iputils-ping \
    gawk \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir prefect dbt-core dbt-postgres pandas sqlalchemy psycopg2-binary openpyxl pyxlsb

EXPOSE 4200
CMD ["prefect", "server", "start", "--host", "0.0.0.0"]
