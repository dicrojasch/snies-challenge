# Implement 3NF Silver Layer for SNIES Data

This plan aims to establish a properly normalized Entity-Relationship Model (3NF) for the Silver Layer in the `snies-challenge` dbt project. It takes two raw tables from the Bronze layer (`students` and `teachers`) and models them into a robust schema using Inmon's methodology.

## User Review Required

> [!IMPORTANT]
> - Do you already have `dbt_utils` configured in `packages.yml`? It will be required for the `generate_surrogate_key` function in the transaction/intersection tables.
> - For fields like `nivel_cine` in `teachers` vs `id_cine_campo_amplio` etc. in `students`, I am separating them slightly (`cine_classifications` handles the nested IDs for students, while we will leave `cine_level` directly on the `teacher_records` table if it does not have a formal ID, or we can create a text-based dimension for `cine_level`). Please confirm this approach.

## Proposed Changes

We will create a structured hierarchy in your `dbt_project/models` consisting of a `staging` layer to standardize raw views and a `silver` layer to materialize the 3NF relationships.

---

### Sources Configuration (Bronze)

Update the `sources.yml` to define the raw tables that match the provided schemas for `students` and `teachers`.

#### [MODIFY] dbt_project/models/silver/sources.yml
Update the source definitions to include `students` and `teachers` within the `bronze` schema.

---

### Staging Layer

We will introduce a `staging` directory to cast and alias everything to English `snake_case`.

#### [NEW] dbt_project/models/staging/stg_students.sql
Standardize column names for students and handle any data type casting. Extracts columns like `código_de_la_institución` into `institution_id`.

#### [NEW] dbt_project/models/staging/stg_teachers.sql
Standardize column names for teachers, translating metadata such as `máximo_nivel_de_formación_del_docente` into `formation_level_name`.

---

### Silver Layer (Dimensions)

We will create distinct dimension tables that deduplicate descriptive data, ensuring every descriptive attribute belongs to a single dimensional entity with its own primary key.

#### [NEW] dbt_project/models/silver/dim_sectors.sql
PK: `sector_id`. Name: `sector_name`.

#### [NEW] dbt_project/models/silver/dim_characters.sql
PK: `character_id`. Name: `character_name`.

#### [NEW] dbt_project/models/silver/dim_geography.sql
PK: `municipality_id`. Includes `municipality_name`, `department_id`, and `department_name`. Combines locations from institutions and programs.

#### [NEW] dbt_project/models/silver/dim_institutions.sql
PK: `institution_id`. Includes `parent_institution_id`, `institution_name`, and foreign keys to `dim_sectors`, `dim_characters`, and `dim_geography`.

#### [NEW] dbt_project/models/silver/dim_academic_levels.sql
PK: `academic_level_id`. Name: `academic_level_name`.

#### [NEW] dbt_project/models/silver/dim_formation_levels.sql
PK: `formation_level_id`. Name: `formation_level_name`. Contains combined derivation from both students and teachers.

#### [NEW] dbt_project/models/silver/dim_cine_classifications.sql
PK: `cine_detailed_field_id`. Contains nested hierarchy of specific and broad fields.

#### [NEW] dbt_project/models/silver/dim_academic_programs.sql
PK: `program_id`. Includes `program_name` and foreign keys to levels, fields, and methodologies.

#### [NEW] dbt_project/models/silver/dim_gender.sql
PK: `gender_id`. Name: `gender_name`. Derived from both data sources.

#### [NEW] dbt_project/models/silver/dim_contract_types.sql
PK: `contract_type_id`. Name: `contract_type_name`. From teachers' source.

#### [NEW] dbt_project/models/silver/dim_dedication_times.sql
PK: `dedication_id`. Name: `dedication_name`. From teachers' source.

---

### Silver Layer (Transactions / Intersections)

Replaces the monolithic facts with associative tables that hold entirely foreign keys and the relevant quantitative metrics.

#### [NEW] dbt_project/models/silver/student_enrollment_records.sql
Uses surrogate key for the PK. Only holds:
`enrollment_record_id` (PK), `institution_id` (FK), `program_id` (FK), `gender_id` (FK), `data_year`, `semester`, and `enrollment_count`.

#### [NEW] dbt_project/models/silver/teacher_records.sql
Uses surrogate key for the PK. Only holds:
`teacher_record_id` (PK), `institution_id` (FK), `gender_id` (FK), `formation_level_id` (FK), `cine_level`, `dedication_id` (FK), `contract_type_id` (FK), `data_year`, `semester`, and `teacher_count`.

---

### Data Contracts and Documentation

To enforce the 3NF and ensure relationships are correctly established.

#### [NEW] dbt_project/models/silver/schema.yml
Contains robust tests mapping all the aforementioned tables to enforce:
- `unique` and `not_null` limits on all defined primary keys.
- `relationships` tests spanning transaction FKs out towards dimensions to enforce referential integrity.

## Verification Plan

### Automated Tests
- Running `dbt test` to ensure that PK/FK constraints aren't violated and surrogate keys compile cleanly.
- Running `dbt compile` to ensure no macro issues with `dbt_utils`.

### Manual Verification
- Visual inspection of the compiled DDL for proper 3NF structure and schema types.
