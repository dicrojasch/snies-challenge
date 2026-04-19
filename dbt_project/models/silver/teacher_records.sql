-- =============================================================================
-- Silver Transaction Table: teacher_records
-- Grain : one row per (institution × gender × formation_level × dedication_time
--                      × contract_type × cine_level × year × semester)
-- PK    : teacher_record_id  (surrogate key via dbt_utils)
-- FKs   : institution_id      → dim_institutions
--         gender_id           → dim_gender
--         formation_level_id  → dim_formation_levels
--         dedication_time_id  → dim_dedication_times
--         contract_type_id    → dim_contract_types
-- Metric: teacher_count (no_de_docentes)
-- =============================================================================

-- models/silver/teacher_records.sql

WITH raw_stg AS (
    SELECT
        institution_id,
        gender_id,
        -- Traemos ID y Nombre para el JOIN con el Seed
        formation_level_id AS src_formation_id,
        formation_level_name AS src_formation_name,
        dedication_time_id AS src_dedication_id,
        dedication_time_name AS src_dedication_name,
        contract_type_id,
        data_year,
        semester,
        teacher_count
    FROM {{ ref('stg_teachers') }}
),

mapped AS (
    SELECT
        r.institution_id,
        r.gender_id,
        COALESCE(fl.target_id, r.src_formation_id, 0) AS formation_level_id,
        COALESCE(dt.target_dedication_id, r.src_dedication_id, 0) AS dedication_time_id,
        r.contract_type_id,
        r.data_year,
        r.semester,
        r.teacher_count
    FROM raw_stg r
    LEFT JOIN {{ ref('map_formation_levels') }} fl
        ON r.src_formation_id = fl.src_id
        AND INITCAP(TRIM(r.src_formation_name)) = INITCAP(TRIM(fl.src_name))
    LEFT JOIN {{ ref('map_dedication_time') }} dt
        ON r.src_dedication_id = dt.source_dedication_id
        AND r.src_dedication_name = dt.source_dedication_name
),

final_grouped AS (
    SELECT
        institution_id,
        gender_id,
        formation_level_id,
        dedication_time_id,
        contract_type_id,
        data_year,
        semester,
        SUM(teacher_count) AS teacher_count
    FROM mapped
    WHERE institution_id IS NOT NULL 
      AND formation_level_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'institution_id',
        'gender_id',
        'formation_level_id',
        'dedication_time_id',
        'contract_type_id',
        'data_year',
        'semester'
    ]) }} AS teacher_record_id,
    *
FROM final_grouped
