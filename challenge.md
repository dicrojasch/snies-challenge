# Technical Challenge: Data Architect

## 1. Introduction
The objective of this assessment is to evaluate your ability to lead the design, implementation, and deployment of an **End-to-End** data ecosystem. We are looking for a professional who does more than just write code—someone who designs scalable architectures, documents processes, and ensures data quality to support strategic decision-making.

---

## 2. The Challenge
The organization requires an automated system to monitor the academic capacity of **Higher Education Institutions (HEIs/IES)** in Bogotá. Using open data from **SNIES** (National Higher Education Information System), you must build a solution to calculate and visualize the **Student-to-Teacher Ratio**.

### Business Specifications:
* **Analysis Period:** 2022 to 2024.
* **Geographic Scope:** HEIs headquartered in the city of Bogotá.
* **Primary Metric:** Number of enrolled students vs. number of teachers.
* **Special Attribute (Plus):** Create a classification to distinguish whether the institution belongs to the **SUE** (State University System) or not.
* **Goal:** Determine the student-to-teacher ratio for Bogotá universities from 2022 to 2024.

---

## 3. Assessment Phases

### Phase A: Ingestion and Orchestration (ETL/ELT)
* Design a workflow that automates the download and processing of SNIES microdata.
* Handle file variability (**Excel/CSV formats**) across different years.
* The use of orchestration tools (**Airflow, Prefect, or similar**) is highly valued. The tool must allow for the ingestion of future periods.

### Phase B: Data Modeling (OLAP)
* Design and build a database schema optimized for analytics.
* Implement a structure that guarantees **data traceability**. The model should allow for high-performance queries by BI tools.

### Phase C: Accessibility and Backend
* To ensure the data is consumable by other applications, ensure the database is accessible from external tools (e.g., **Tableau**).

### Phase D: DevOps and Deployment (Optional)
* The entire solution should be contained within a **Docker** environment.
* Deployment must be reproducible via a `docker-compose.yaml` file.

---

## 4. Deliverables
1. **Source Code:** A link to a repository (GitHub/GitLab) with organized code.
2. **Documentation (README.md):**
    * Diagram of the implemented architecture.
    * Installation and execution guide.
    * Brief explanation of the technical decisions made.
3. **Knowledge Transfer:** A section explaining how you would **scale this solution** if it were necessary to integrate data from the entire country.

---

## 5. Evaluation Criteria
* **Architecture:** Ability to organize data layers effectively.
* **Quality:** Handling of null values, duplicates, and normalization of HEI names.
* **Sustainability:** Clarity in documentation and ease of maintenance.
* **Agility:** Use of AI tools (**Vibe Coding**) to optimize development efficiency.