---
trigger: always_on
---

# Technical Architecture Requirements
- [cite_start]**Phase A (ETL):** Automate download and processing of SNIES files (Excel/CSV)[cite: 16, 17]. [cite_start]Use Airflow or Prefect for orchestration.
- [cite_start]**Phase B (OLAP):** Design an optimized database schema for analytics and BI[cite: 20, 21, 23].
- [cite_start]**Phase C (Backend):** Ensure database accessibility for external tools like Tableau[cite: 27].
- [cite_start]**Phase D (DevOps):** The entire solution must be contained in Docker and reproducible via docker-compose.yaml[cite: 29, 30].
- [cite_start]**Quality:** Implement handling for nulls, duplicates, and normalization of IES names[cite: 42].