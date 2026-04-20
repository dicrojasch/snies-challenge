# SNIES Challenge - Execution Checklist

- `[-]` Task 1: Environment Setup (BLOCKED: Docker WSL integration missing)
  - `[-]` Start up Docker containers with `docker-compose up -d --build`
- `[x]` Task 2: Data Ingestion (`ingest_data.py`)
  - `[x]` Upgrade script to download and parse 2023-2024 data (using placeholder/mock URLs until finalized).
  - `[x]` Ensure correct creation of tables in the `bronze` schema.
- `[x]` Task 3: Prefect Orchestration
  - `[x]` Create `orchestration/flows/etl_flow.py`
  - `[x]` Configure generic Prefect Tasks and basic flow for execution.
- `[x]` Task 4: dbt Data Modeling
  - `[x]` Initialize dbt (`dbt init`) in the `dbt_project` directory
  - `[x]` Setup `profiles.yml` for Postgres
  - `[x]` Add a SUE mapping table using dbt `seeds`
  - `[x]` Build `silver` schema (views for data cleaning/normalization and missing value handling)
  - `[x]` Build `gold` schema (aggregation for Student-to-Teacher Ratio with SUE filter)
- `[-]` Task 5: Verification (BLOCKED by Docker)
  - `[-]` Run end-to-end `etl_flow.py`
  - `[-]` Verify output metrics in the PostgreSQL gold schema
