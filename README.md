# SNIES Data Challenge

Automated system to monitor the academic capacity of Higher Education Institutions (HEIs) in Bogotá using SNIES Open Data.

## 📊 Architecture Overview

The project follows a **Medallion Architecture** (Bronze, Silver, Gold Layers) and uses **Prefect** for orchestration and **dbt** for transformations.

```mermaid
graph TD
    %% Orchestration Layer
    subgraph Orchestration ["<h3>ORCHESTRATION & GOVERNANCE (APACHE AIRFLOW)</h3>"]
        direction TB
        
        subgraph External_Sources ["<h4>PHASE 0: EXTERNAL DATA SOURCES</h4>"]
            Web2023["SNIES Web"]
        end

        subgraph Phase_A ["<h4>PHASE A: INGESTION</h4>"]
            Loader["Loader (ingest_data.py)"]
        end

        subgraph Phase_B ["<h4>PHASE B: DATA MODELING (DBT)</h4>"]
            
            subgraph Bronze_Layer ["<b>BRONZE: RAW DATA</b>"]
                table_docentes["raw_docentes"]
                table_estudiantes["raw_estudiantes"]
            end

            dbt_transform_silver{{"dbt run / build"}}

            subgraph Silver_Layer ["<b>SILVER: NORMALIZED DATA</b>"]
                stg_docentes["stg_docentes"]
                stg_estudiantes["stg_estudiantes"]
                sue_seed["sue_institutions (Seed)"]
            end

            dbt_transform_gold{{"dbt run / build"}}

            subgraph Gold_Layer ["<b>GOLD: ANALYTICAL DATA</b>"]
                final_metrics["student_teacher_ratio"]
            end
        end

        subgraph Consumption ["<h4>PHASE C: ACCESSIBILITY</h4>"]
            BI["Tableau / BI Tools"]
        end
    end

    %% Data Flow & Orchestration Links
    Web2023 --> Loader
    Loader --> table_docentes
    Loader --> table_estudiantes

    %% DBT Transitions
    table_docentes & table_estudiantes --> dbt_transform_silver
    dbt_transform_silver --> stg_docentes & stg_estudiantes
    
    stg_docentes & stg_estudiantes & sue_seed --> dbt_transform_gold
    dbt_transform_gold --> final_metrics

    final_metrics --> BI

    %% Styling
    style Orchestration fill:#f9f9f9,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5
    style dbt_transform_silver fill:#ff6b6b,stroke:#fff,color:#fff
    style dbt_transform_gold fill:#ff6b6b,stroke:#fff,color:#fff
```

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- Docker Desktop with **WSL 2 Integration** enabled.

### Execution
1.  **Build and Start Containers:**
    ```bash
    docker compose up -d --build
    ```
2.  **Verify Services:**
    - Prefect UI: `http://localhost:4200`
    - Postgres: `localhost:5432`

## 🛠 Tech Stack
- **Database:** PostgreSQL (OLAP)
- **Orchestration:** Prefect
- **Data Transformation:** dbt-core
- **Ingestion:** Python (Pandas + SQLAlchemy)
- **DevOps:** Docker & Docker Compose

## 📈 Key Metrics
- **Student-to-Teacher Ratio:** Calculated across HEIs in Bogotá (2022-2024).
- **SUE Classification:** Identification of State University System institutions.
