# SNIES Prefect Task Tests

This directory contains isolated test scripts designed to validate the individual tasks defined in `etl_flow.py`. These scripts wrap each task in a temporary Prefect `@flow` to allow for native logging and state management observation without running the full end-to-end pipeline.

## Prerequisites

These scripts are intended to be executed from **inside** the orchestrator/loader container to ensure all environment variables (like `DATABASE_URL`) and dependencies are correctly configured.

## Execution Instructions

### 1. Access the Container
First, identify your running orchestrator container and open a bash session:

```bash
docker exec -it <container_name_or_id> bash
```

### 2. Run the Test Scripts
Navigate to the repository root inside the container (usually `/usr/src/app`) and execute the scripts using Python:

#### Test Data Fetching
Validates the execution of the shell script and file presence in the raw directory.
```bash
python orchestration/flows/tests/test_fetch.py
```

#### Test File Auditing
Validates the hash comparison logic against the database `bronze.ingestion_audit` table.
```bash
python orchestration/flows/tests/test_audit.py
```

#### Test Data Ingestion
Validates the Pandas ingestion logic using a sample 2022 file. This will output the DataFrame schema and first 5 rows to the console.
```bash
python orchestration/flows/tests/test_ingestion.py
```

## Summary of Test Scripts

| File | Task Tested | Description |
| :--- | :--- | :--- |
| `test_fetch.py` | `fetch_external_data` | Downloads raw SNIES files and verifies storage. |
| `test_audit.py` | `file_audit` | Detects new or modified files based on SHA256 hashes. |
| `test_ingestion.py` | `run_ingestion` | Processes an Excel file, logs data profiles, and loads into Postgres. |

> [!TIP]
> Use these scripts to debug logic changes in `etl_flow.py` or `ingest_data.py` before committing to a full deployment.
