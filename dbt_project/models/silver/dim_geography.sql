-- =============================================================================
-- Silver Dimension: dim_geography
-- 3NF Entity: Municipalities and their parent Departments.
-- Combines IES location and programme offer location from students,
-- plus IES location from teachers.  Municipality code is the PK.
-- =============================================================================

WITH ies_geo_students AS (
    SELECT DISTINCT
        ies_municipality_id   AS municipality_id,
        ies_municipality_name AS municipality_name,
        ies_department_id     AS department_id,
        ies_department_name   AS department_name
    FROM {{ ref('stg_students') }}
    WHERE ies_municipality_id IS NOT NULL
),

program_geo AS (
    SELECT DISTINCT
        program_municipality_id   AS municipality_id,
        program_municipality_name AS municipality_name,
        program_department_id     AS department_id,
        program_department_name   AS department_name
    FROM {{ ref('stg_students') }}
    WHERE program_municipality_id IS NOT NULL
),

ies_geo_teachers AS (
    SELECT DISTINCT
        ies_municipality_id   AS municipality_id,
        ies_municipality_name AS municipality_name,
        ies_department_id     AS department_id,
        ies_department_name   AS department_name
    FROM {{ ref('stg_teachers') }}
    WHERE ies_municipality_id IS NOT NULL
),

unioned AS (
    SELECT * FROM ies_geo_students
    UNION
    SELECT * FROM program_geo
    UNION
    SELECT * FROM ies_geo_teachers
)

SELECT
    municipality_id,
    INITCAP(TRIM(municipality_name)) AS municipality_name,
    department_id,
    INITCAP(TRIM(department_name))   AS department_name
FROM unioned
