-- =============================================================================
-- Silver Transaction Table: teacher_records
-- Grain : one row per (institution × gender × formation_level × dedication_type
--                      × contract_type × cine_level × year × semester)
-- PK    : teacher_record_id  (surrogate key via dbt_utils)
-- FKs   : institution_id      → dim_institutions
--         gender_id           → dim_gender
--         formation_level_id  → dim_formation_levels
--         dedication_type_id  → dim_dedication_types
--         contract_type_id    → dim_contract_types
-- NOTE  : cine_level_id kept as text attribute — teachers source has no
--         numeric CINE ID, so it cannot FK to dim_cine_classifications.
-- Metric: teacher_count (no_de_docentes)
-- =============================================================================

WITH stg AS (
    SELECT
        institution_id,
        gender_id,
        formation_level_id,
        dedication_type_id,
        contract_type_id,
        cine_level_id,
        data_year,
        semester,
        SUM(teacher_count) AS teacher_count
    FROM {{ ref('stg_teachers') }}
    WHERE institution_id    IS NOT NULL
      AND gender_id         IS NOT NULL
      AND formation_level_id IS NOT NULL
      AND dedication_type_id IS NOT NULL
      AND contract_type_id  IS NOT NULL
      AND data_year         IS NOT NULL
      AND semester          IS NOT NULL
    GROUP BY
        institution_id,
        gender_id,
        formation_level_id,
        dedication_type_id,
        contract_type_id,
        cine_level_id,
        data_year,
        semester
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'institution_id',
        'gender_id',
        'formation_level_id',
        'dedication_type_id',
        'contract_type_id',
        'cine_level_id',
        'data_year',
        'semester'
    ]) }}                   AS teacher_record_id,
    institution_id,
    gender_id,
    formation_level_id,
    dedication_type_id,
    contract_type_id,
    cine_level_id,
    data_year,
    semester,
    teacher_count
FROM stg
