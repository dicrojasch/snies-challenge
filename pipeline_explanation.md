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

### 🔍 Data Traceability & Integrity
Traceability is guaranteed by the Medallion architecture, where every transformation is tracked from the raw source to the final metric. 
- **Lineage**: Automated by **dbt**, allowing you to visualize the exact path of any data point.
- **Audit Trails**: A `bronze.ingestion_audit` table tracks file hashes, timestamps, and row counts for every ingestion event.
- **Verification**: dbt tests run at every layer to ensure data remains consistent and valid during its journey.

---

## 2. Pipeline Orchestration

The entire workflow is orchestrated using **Prefect**, which allows for task hierarchy management, centralized monitoring, and idempotent execution. For a high-level view, see [architecture_diagram.md](file:///home/diego/repos/snies-challenge/architecture_diagram.md).

The pipeline consists of four main tasks:

### Task 1: Raw Data Acquisition (Bash Script)
- **File**: `scripts/get_data.sh`
- **Logic**: A high-performance Bash script that queries the Ministry of Education's website and downloads the "Bases Consolidadas" (consolidated bases).
- **Key Feature**: It detects existing files in the local directory to avoid redundant downloads, ensuring it only fetches new data.

### Task 2: Intelligent File Filtering
- **Logic**: Implemented within the Prefect flow using the [ingestion_config.json](file:///home/diego/repos/snies-challenge/orchestration/flows/ingestion_config.json) file to establish filtering criteria.
- **Configuring New Data**:
    - **Add New Years**: Update the `allowed_years` list in the config file.
    - **Add New File Types**: Add the expected filename prefix to the `allowed_prefixes` list.
- **Filtering Logic**:
    - **Naming Convention**: Matches files against the allowed prefixes (currently `estudiantes_matriculados` and `docentes`).
    - **Year Range**: Filters files by the years specified in the configuration.
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

#### Configuration
The system uses Docker Secrets for secure credential management. Before starting the infrastructure, create the `secrets` directory and define your database credentials:

```bash
mkdir -p secrets
echo "snies_user" > secrets/db_user
echo "snies_password" > secrets/db_password
```

#### Infrastructure Startup
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

## 4. Connectivity & External Tools

To connect external BI tools (like Tableau or PowerBI) or SQL clients to the database, use the following parameters:

| Parameter | Value |
| :--- | :--- |
| **Host** | `localhost` |
| **Port** | `5432` |
| **Database** | `snies` |
| **Username** | (Refer to `secrets/db_user`) |
| **Password** | (Refer to `secrets/db_password`) |

> [!NOTE]
> If connecting from another container within the same Docker network, use the service name `postgres` as the host.

---

## 5. Scalability & Future Roadmap

While the current architecture (Prefect + dbt + PostgreSQL) is robust enough to handle significant data volumes, scaling the solution to integrate data for the entire country would involve transitioning to a cloud-native Big Data stack:

1.  **Storage**: Migrate to a cloud data warehouse like **Snowflake** or **Amazon Redshift** for elastic scaling and high-performance analytical queries.
2.  **Processing**: Utilize **AWS Glue** or **managed Spark** for distributed processing of massive datasets.
3.  **Messaging & Streaming**: Implement **Amazon SQS** for task queuing or **Amazon Kinesis/Kafka** for real-time data streaming and ingestion.
4.  **Orchestration**: Transition to a managed orchestration service like **Prefect Cloud** or **MWAA**.

The current solution is designed to scale horizontally and can already support a significant amount of data by increasing container resources.

---

## 6. AI-Powered Development (Vibe Code)

This solution was implemented using the **Antigravity AI Agent**, primarily utilizing the **Gemini 3 Flash** model. Due to token limitations and complexity requirements, **Gemini 3.1 Pro** and **Claude 3.5 Sonnet** were also used for specific architectural decisions.

### Development Methodology
- **Implementation Plans**: Every complex change started with a blueprint (blueprint técnico) generated by the agent.
- **Structured Tasks**: High-level objectives were broken down into actionable items.
- **Walkthroughs**: Final delivery reports documenting changes and verification results.
- **Optimized Prompting**: The process involved using the Gemini web interface (Fast mode for queries, Thinking for intermediate logic, and Pro for high complexity) to optimize token consumption and ensure precision.

Prompts and plans are stored in the `./implementation_plans` directory for historical reference.

---

## 7. Key File Reference

| Resource | Path | Description |
| :--- | :--- | :--- |
| **ETL Flow** | `orchestration/flows/etl_flow.py` | Main Prefect entry point. |
| **Config** | `orchestration/flows/ingestion_config.json` | Rules for file filtering. |
| **Downloader** | `scripts/get_data.sh` | Bash script for file extraction. |
| **Ingestion Script** | `scripts/ingest_data.py` | Python logic for Excel-to-DB loading. |
| **dbt Models** | `dbt_project/models/` | SQL transformations logic. |
| **Normalization Rules** | `dbt_project/seeds/` | CSV files for entity standardization. |
| **Infrastructure** | `docker-compose.yaml` | Service orchestration (Postgres, Prefect, Loader). |

Co-authored-by: Diego <diego@example.com>
Co-authored-by: Antigravity <ai-agent@google.com>
