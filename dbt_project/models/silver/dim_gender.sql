-- =============================================================================
-- Silver Dimension: dim_gender
-- 3NF Entity: Gender (Masculino / Femenino) present in both source tables.
-- =============================================================================

WITH gender_from_students AS (
    SELECT DISTINCT
        gender_id,
        gender_name
    FROM {{ ref('stg_students') }}
    WHERE gender_id IS NOT NULL
),

gender_from_teachers AS (
    SELECT DISTINCT
        gender_id,
        gender_name
    FROM {{ ref('stg_teachers') }}
    WHERE gender_id IS NOT NULL
),

unioned AS (
    SELECT * FROM gender_from_students
    UNION
    SELECT * FROM gender_from_teachers
)

SELECT
    gender_id,
    INITCAP(TRIM(gender_name)) AS gender_name
FROM unioned
