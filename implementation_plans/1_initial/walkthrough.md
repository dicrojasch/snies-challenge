# Phase A & B Implementation Walkthrough

Awesome progress! The codebase for Phase A and B has just been completed.
Our primary blocker for executing the tests right now is the missing Docker Engine running on the local host (WSL 2 Integration needs to be activated in your Docker Desktop). However, the architectural components are fully mapped and ready to operate once Docker is available.

## Key Accomplishments

### 1. Ingestion Engine Pipeline (`ingest_data.py`)
- Configured dynamic detection of static SNIES datasets from the `/files` directory.
- Upgraded the script with a Python-based mechanism to download **2023 - 2024** data from static endpoints using the `requests` library. Currently pointing to mocks; just swap the URLs when the exact links are obtained.
- Enforced a standard `snake_case` mechanism for all Dataframe columns to avoid formatting problems with different spreadsheet headers.
- **Output:** Saves to the `bronze` Schema via `pandas.to_sql`.

### 2. Orchestration with Prefect (`etl_flow.py`)
- We implemented a programmatic python flow that replaces Cron.
- Contains modular tasks `run_ingestion()` and `run_dbt()`.
- Built to run seamlessly within the `Orchestrator` Docker container, executing shell commands to initialize data and subsequently start building the metrics using dbt.

### 3. Data Transformation (`dbt_project`)
The complete Medallion logic has been orchestrated inside `dbt` and tracked via the configured schema `profiles.yml` mapping directly to PostgreSQL:
- **Silver Schema (Cleaning):** Extract models `stg_estudiantes.sql` and `stg_docentes.sql` reading out of `bronze` definitions in `sources.yml`. Casts types strictly, renames raw Colombian headers, and collapses by year and institution ID dynamically.
- **Seeds (SUE Identification):** Hardcoded a specific SUE dimension table inside `dbt_project/seeds/sue_institutions.csv` which automatically uploads as a table to evaluate and tag whether the Institution belongs to the state network.
- **Gold Schema (Analytics):** Built the final master table model `student_teacher_ratio.sql`.
  - Applies a `FULL OUTER JOIN` to blend student and teacher registries securely.
  - Generates the crucial challenge calculation: **Enrolled Students vs. Teachers**.
  - Appends the Boolean Flag indicator (`is_sue_institution`).

## Next Steps
> [!WARNING]
> Please turn on your Docker Desktop (and verify WSL integration inside its settings) to spin up the Database server.

Once your Docker engine is active:
1. Run `docker compose up -d --build`
2. Test the models natively by executing Python inside the orchestrator container.
