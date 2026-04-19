# 3NF Silver Layer Implementation Walkthrough

I have successfully designed and implemented a Normalized Entity-Relationship Model (3NF) for the Silver Layer of the Data Lakehouse using dbt. This implementation follows Inmon's methodology and ensures high data quality through strict referential integrity.

## Changes Implemented

### 1. Project Configuration
- **[packages.yml](file:///home/diego/repos/snies-challenge/dbt_project/packages.yml)**: Added `dbt-labs/dbt_utils` for surrogate key generation.
- **[dbt_project.yml](file:///home/diego/repos/snies-challenge/dbt_project/dbt_project.yml)**: Configured schema-specific materializations (Staging as Views, Silver/Gold as Tables) and fixed legacy YAML errors.
- **[sources.yml](file:///home/diego/repos/snies-challenge/dbt_project/models/silver/sources.yml)**: Defined exact Bronze source schemas for `students` and `teachers`.

### 2. Staging Layer (Renaming & Cleaning)
- **[stg_students.sql](file:///home/diego/repos/snies-challenge/dbt_project/models/staging/stg_students.sql)**: Translated all Spanish columns to English `snake_case` and cast types.
- **[stg_teachers.sql](file:///home/diego/repos/snies-challenge/dbt_project/models/staging/stg_teachers.sql)**: Standardized teacher data, handling unique fields like `cine_level`.

### 3. Silver Layer (3NF Normalization)
I decomposed the flat tables into 11 distinct entities:
- **Dimensions**: `dim_institutions`, `dim_academic_programs`, `dim_geography`, `dim_sectors`, `dim_characters`, `dim_formation_levels`, `dim_academic_levels`, `dim_cine_classifications`, `dim_gender`, `dim_contract_types`, and `dim_dedication_types`.
- **Transactions**: 
    - `student_enrollment_records`: Intersection table for enrollment metrics.
    - `teacher_records`: Intersection table for teacher metrics.

> [!NOTE]
> All primary keys use source integer IDs where available. Transaction tables use `dbt_utils.generate_surrogate_key` for unique record identification.

### 4. Quality & Documentation
- **[schema.yml](file:///home/diego/repos/snies-challenge/dbt_project/models/silver/schema.yml)**: Implemented 102 tests, including `unique`, `not_null`, and `relationships` to enforce referential integrity between fact and dimensions.
- Refactored all tests to adhere to **dbt 1.11+ syntax** requirements (nesting arguments under the `arguments` key).

### 5. Gold Layer Refactoring
- **[student_teacher_ratio.sql](file:///home/diego/repos/snies-challenge/dbt_project/models/gold/student_teacher_ratio.sql)**: Rewrote the flagship analytical model to join the normalized Silver entities. It now strictly filters for Bogotá-based IES and calculates the required metric.

## Verification Results

### `dbt compile`
Project compiles cleanly with 16 models and 102 tests detected.

### Data Model Integrity
- Verified that `is_sue_member` business rule is correctly implemented in `dim_institutions`.
- Confirmed that `dim_geography` handles both IES and Program locations.
- Transaction tables successfully aggregate to the required grain before surrogate key calculation.
