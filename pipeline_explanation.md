# SNIES Data Pipeline Documentation

This documentation provides an overview of the ETL pipeline designed to monitor the academic capacity of Higher Education Institutions (IES) in Bogotá. The architecture follows a modern **Medallion Architecture** pattern, orchestrated via **Prefect** and transformed using **dbt**.

## 1. Data Architecture: The Medallion Pattern

We implement a three-layer architecture to ensure data quality, consistency, and analytical performance. For a visual representation, please refer to [models.md](file:///home/diego/repos/snies-challenge/models.md).

### 🟤 Bronze Layer (Raw)
- **Structure**: Nearly identical to the source Excel/CSV files.
- **Purpose**: Serves as a raw landing zone. Data is ingested "as-is" to maintain a faithful history of the source information.
- **Storage**: `bronze` schema in PostgreSQL.

### 🥈 Silver Layer (Normalized - Snowflake Schema)
- **Structure**: A fully normalized dimensional model following a **Snowflake Schema**.
- **Purpose**: This layer enforces data integrity, unique constraints, and relationship consistency. Normalization allows for rigorous controls over data entities (Institutions, Geography, Programs, etc.).
- **Storage**: `silver` schema.

### 🥇 Gold Layer (Aggregated - Star Schema)
- **Structure**: A de-normalized **Star Schema**.
- **Purpose**: Optimized for business intelligence and reporting. It contains pre-calculated metrics such as the **Teacher-to-Student Ratio**.
- **Storage**: `gold` schema.

---

## 2. Pipeline Orchestration

The entire workflow is orchestrated using **Prefect**, which allows for task hierarchy management, centralized monitoring, and idempotent execution. For a high-level view, see [architecture_diagram.md](file:///home/diego/repos/snies-challenge/architecture_diagram.md).

The pipeline consists of four main tasks:

### Task 1: Raw Data Acquisition (Bash Script)
- **File**: `scripts/get_data.sh`
- **Logic**: A high-performance Bash script that queries the Ministry of Education's website and downloads the "Bases Consolidadas" (consolidated bases).
- **Key Feature**: It detects existing files in the local directory to avoid redundant downloads, ensuring it only fetches new data.

### Task 2: Intelligent File Filtering
- **Logic**: Implemented within the Prefect flow using the [ingestion_config.json](file:///home/diego/repos/snies-challenge/orchestration/flows/ingestion_config.json) file to establish filtering criteria:
    - **Naming Convention**: Allows setting the prefixes of files to be processed (currently `estudiantes_matriculados` and `docentes`).
    - **Year Range**: Allows establishing the years to process (currently **2022, 2023, and 2024**).
    - **Audit Check**: Queries the database to verify if a file has already been successfully loaded previously. Only new files are passed to the next stage.

### Task 3: Bronze Ingestion Engine
- **File**: `scripts/ingest_data.py`
- **Logic**: Handles the technical complexity of loading heterogeneous Excel files into PostgreSQL.
- **Capabilities**:
    - **Format Agnostic**: Supports `.xlsx`, `.xlsb`, and `.csv`.
    - **Dynamic Tab Detection**: Automatically identifies and navigates to the correct data tab within multi-sheet workbooks.
    - **Metadata Handling**: Automatically detects the start of the data table, ignoring top rows containing metadata/headers.
    - **Audit Control**: Registers processed files in a control table to prevent re-processing in future runs.

### Task 4: dbt Transformations & Quality Assurance
- **Directory**: `dbt_project/`
- **Logic**: Moves data from **Bronze → Silver → Gold** using SQL-based transformations.
- **Quality Controls**:
    - **dbt Tests**: Validates data types, ensures columns are not null, and verifies predefined value ranges.
    - **Standardization (Seeds)**: Uses dbt seeds (CSV files) to unify institution names and IDs, handling inconsistencies like trailing spaces or typos.

---

## 3. Operational Guide

### Deployment
To start the infrastructure, run:
```bash
docker-compose up -d
```

### Running the Pipeline
To trigger the full ETL process (from download to Gold layer):
```bash
docker exec snies-challenge-orchestrator-1 python /orchestration/flows/etl_flow.py
```

### Monitoring via Prefect
Once the services are running, access the Prefect dashboard at [http://localhost:4200](http://localhost:4200):
- **Dashboard**: [http://localhost:4200/dashboard](http://localhost:4200/dashboard) - Executive statistics.
- **Flows**: [http://localhost:4200/flows](http://localhost:4200/flows) - View registered pipeline definitions.
- **Runs**: [http://localhost:4200/runs](http://localhost:4200/runs) - Real-time logs and status (Running, Completed, Failed).

### Exploring the Data Model with dbt
After a successful execution, you can visualize the data lineage and schema documentation:
1. **Start the dbt docs server**:
   ```bash
   docker exec -w /dbt_project snies-challenge-orchestrator-1 dbt docs serve --port 8080 --host 0.0.0.0
   ```
2. **Access the UI**: [http://localhost:8080/](http://localhost:8080/)
3. **Key Views**:
   - **Lineage Graph**: [http://localhost:8080/#!/overview?g_v=1](http://localhost:8080/#!/overview?g_v=1) - See the flow from Bronze → Staging → Silver → Gold (`student_teacher_ratio`).
   - **Bronze Schema**: [http://localhost:8080/#!/source_list/bronze](http://localhost:8080/#!/source_list/bronze) - Inspect raw table structures.
   - **Silver/Gold Models**: Navigate to `snies_challenge/models` to explore the normalized and aggregated definitions.

---

## 4. Key File Reference

| Resource | Path | Description |
| :--- | :--- | :--- |
| **ETL Flow** | `orchestration/flows/etl_flow.py` | Main Prefect entry point. |
| **Downloader** | `scripts/get_data.sh` | Bash script for file extraction. |
| **Ingestion Script** | `scripts/ingest_data.py` | Python logic for Excel-to-DB loading. |
| **dbt Models** | `dbt_project/models/` | SQL transformations logic. |
| **Normalization Rules** | `dbt_project/seeds/` | CSV files for entity standardization. |
| **Infrastructure** | `docker-compose.yaml` | Service orchestration (Postgres, Prefect, Loader). |

Co-authored-by: Diego <diego@example.com>
Co-authored-by: Antigravity <ai-agent@google.com>
