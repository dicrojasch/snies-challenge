# SNIES Data Challenge

Automated system to monitor the academic capacity of Higher Education Institutions (HEIs) in Bogotá using SNIES Open Data.

## 📊 Architecture Overview

The project follows a **Medallion Architecture** (Bronze, Silver, Gold Layers) and uses **Prefect** for orchestration and **dbt** for transformations.

```mermaid
graph TD
    %% Orchestration Layer
    subgraph Orchestration ["ORCHESTRATION & GOVERNANCE (PREFECT)"]
        direction TB
        
        subgraph External_Sources ["PHASE 0: EXTERNAL DATA SOURCES"]
            Web2023["SNIES Web"]
        end

        subgraph Phase_A ["PHASE A: INGESTION"]
            Loader["Loader (ingest_data.py)"]
        end

        subgraph Phase_B ["PHASE B: DATA MODELING (DBT)"]
            
            subgraph Bronze_Layer ["BRONZE: RAW DATA"]
                table_docentes["raw_docentes"]
                table_estudiantes["raw_estudiantes"]
            end

            dbt_transform_silver{{"dbt run / build"}}

            subgraph Silver_Layer ["SILVER: NORMALIZED DATA"]
                stg_docentes["stg_docentes"]
                stg_estudiantes["stg_estudiantes"]
                sue_seed["sue_institutions (Seed)"]
            end

            dbt_transform_gold{{"dbt run / build"}}

            subgraph Gold_Layer ["GOLD: ANALYTICAL DATA"]
                final_metrics["student_teacher_ratio"]
            end
        end

        subgraph Consumption ["PHASE C: ACCESSIBILITY"]
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
