-- =============================================================================
-- Gold Table: student_teacher_ratio
-- Purpose : Core analytical output — enrolled students vs. teachers per IES,
--           per year and semester, filtered to Bogotá.
-- Business Rules:
--   1. Aggregate at institution × year × semester grain.
--   2. Compute ratio: enrolled / teachers. NULL when no teachers recorded.
--   3. Include is_sue_member flag from dim_institutions.
--   4. Filter: only IES whose municipality is Bogotá (DANE code 11001).
-- Dependencies: Silver layer (student_enrollment_records, teacher_records,
--               dim_institutions, dim_geography).
-- =============================================================================

WITH bogota_institutions AS (
    -- Business Rule: filter to IES domiciled in Bogotá (DANE 11001)
    SELECT i.institution_id
    FROM {{ ref('dim_institutions') }}  i
    JOIN {{ ref('dim_geography') }}     g ON i.municipality_id = g.municipality_id
    WHERE g.municipality_id = 11001
),

enrolled AS (
    SELECT
        e.institution_id,
        e.data_year,
        e.semester,
        SUM(e.enrollment_count) AS total_enrolled
    FROM {{ ref('student_enrollment_records') }} e
    JOIN bogota_institutions b ON e.institution_id = b.institution_id
    GROUP BY e.institution_id, e.data_year, e.semester
),

teachers AS (
    SELECT
        t.institution_id,
        t.data_year,
        t.semester,
        SUM(t.teacher_count) AS total_teachers
    FROM {{ ref('teacher_records') }} t
    JOIN bogota_institutions b ON t.institution_id = b.institution_id
    GROUP BY t.institution_id, t.data_year, t.semester
),

combined AS (
    SELECT
        COALESCE(e.institution_id, t.institution_id) AS institution_id,
        COALESCE(e.data_year,      t.data_year)      AS data_year,
        COALESCE(e.semester,       t.semester)       AS semester,
        COALESCE(e.total_enrolled, 0)                AS total_enrolled,
        COALESCE(t.total_teachers, 0)                AS total_teachers
    FROM enrolled e
    FULL OUTER JOIN teachers t
        ON  e.institution_id = t.institution_id
        AND e.data_year      = t.data_year
        AND e.semester       = t.semester
)

SELECT
    c.institution_id,
    i.institution_name,
    i.is_sue_member,
    c.data_year,
    c.semester,
    c.total_enrolled,
    c.total_teachers,
    -- Business Rule: Student-to-Teacher Ratio
    CASE
        WHEN c.total_teachers > 0
            THEN CAST(c.total_enrolled AS FLOAT) / c.total_teachers
        ELSE NULL
    END AS student_to_teacher_ratio
FROM combined c
JOIN {{ ref('dim_institutions') }} i ON c.institution_id = i.institution_id
ORDER BY c.data_year, c.semester, c.institution_id
