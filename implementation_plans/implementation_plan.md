# SNIES Challenge - Phase A & B Implementation Plan

This plan details the next steps to continue building out the data pipeline now that the foundational infrastructure is established. We will focus on finalizing the raw ingestion and configuring the Prefect orchestrator with dbt.

## User Review Required

> [!IMPORTANT]
> The source URLs for the 2023 and 2024 SNIES open datasets are currently undefined in the script. Do you have specific direct URLs from the Ministry of Education / Datos Abiertos that I should use, or should I attempt to scrape/access a specific API for them?
> 
> Also, please confirm if you want me to automatically spin up the Docker containers and proceed with the dbt initialization (`dbt init`) directly, or if you prefer to review the data models first.

## Proposed Changes

### 1. Finalize Data Ingestion (`ingest_data.py`)
- Implement a robust download mechanism using `requests` to fetch 2023 and 2024 data (Excel/CSV).
- Normalize the incoming files by addressing format variance, renaming headers to standard python snake_case, and ensuring datatypes are properly inferred before loading to the `bronze` schema.

### 2. Configure Orchestration (Prefect)
- Introduce a new file `orchestration/flows/etl_flow.py`.
- Define a Prefect task that runs the loader script.
- Define a Prefect task that executes `dbt build`.
- Link them in a `snies_main_flow` function.

### 3. Initialize Data Transformation (dbt)
- Set up a standard dbt project in the `dbt_project` folder with `dbt init`.
- Configure `profiles.yml` to connect to our local PostgreSQL database using the provided credentials (`user`, `password`, `snies`).
- Create raw sources mapping to the `bronze` schema.
- **Silver Schema (Normalized):** Create foundational models that harmonize HEI (IES) names, handle missing values, and structure relationships (e.g., separating institutions from metrics).
- **Gold Schema (Dimensional):** Create final reporting views, explicitly calculating the **Student-to-Teacher Ratio** and classifying **SUE** institutions perfectly for Tableau.

## Open Questions

- What specific metric dictates whether an Institution is considered part of the **SUE** (Sistema Universitario Estatal)? Should this be hardcoded based on an institution index, or is it a flag present in the raw files?
- The challenge requires distinguishing SUE institutions. If there isn't a direct indicator in the files, we may need a mapping table.

## Verification Plan

### Automated Tests
- `docker-compose up --build -d` ensures all services start correctly.
- Prefect flows will execute locally in the orchestrator container to verify logging.
- `dbt build` will run to test that models compile correctly and data is materialized in Postgres.

### Manual Verification
- Output database schemas (`bronze`, `silver`, `gold`) will be queried directly using `sqlalchemy` to confirm table instantiation and metric correctness.
