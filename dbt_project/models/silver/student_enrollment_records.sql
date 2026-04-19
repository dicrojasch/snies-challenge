-- =============================================================================
-- Silver Transaction Table: student_enrollment_records
-- Grain : one row per (institution × programme × gender × year × semester)
-- PK    : enrollment_record_id  (surrogate key via dbt_utils)
-- FKs   : institution_id → dim_institutions
--         program_id     → dim_academic_programs
--         gender_id      → dim_gender
-- Metric: enrollment_count (inscritos)
-- =============================================================================

WITH stg AS (
    SELECT
        institution_id,
        program_id,
        gender_id,
        data_year,
        semester,
        SUM(enrollment_count) AS enrollment_count
    FROM {{ ref('stg_students') }}
    WHERE institution_id IS NOT NULL
      AND program_id     IS NOT NULL
      AND gender_id      IS NOT NULL
      AND data_year      IS NOT NULL
      AND semester       IS NOT NULL
    GROUP BY
        institution_id,
        program_id,
        gender_id,
        data_year,
        semester
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'institution_id',
        'program_id',
        'gender_id',
        'data_year',
        'semester'
    ]) }}                AS enrollment_record_id,
    institution_id,
    program_id,
    gender_id,
    data_year,
    semester,
    enrollment_count
FROM stg
