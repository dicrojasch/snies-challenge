-- =============================================================================
-- Silver Dimension: dim_geography
-- 3NF Entity: Municipalities and their parent Departments.
-- Combines IES location and programme offer location from students,
-- plus IES location from teachers.  Municipality code is the PK.
-- =============================================================================

-- models/silver/dim_geography.sql

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
),

cleaned_names AS (
    SELECT DISTINCT
        u.municipality_id,
        -- Use the Seed name if available, otherwise clean the source name
        COALESCE(map.target_municipality_name, INITCAP(TRIM(u.municipality_name))) AS municipality_name,
        COALESCE(map.target_department_id, u.department_id) AS department_id,
        COALESCE(map.target_department_name, INITCAP(TRIM(u.department_name))) AS department_name
    FROM unioned u
    LEFT JOIN {{ ref('map_geography') }} map
        ON u.municipality_id = map.municipality_id
)

-- Final SELECT DISTINCT to collapse cases like 'Cali' and 'Santiago de Cali'
SELECT
    municipality_id,
    -- Ensure "D.c." becomes "D.C." if needed via REPLACE
    REPLACE(municipality_name, 'D.c.', 'D.C.') AS municipality_name,
    department_id,
    REPLACE(department_name, 'D.c.', 'D.C.') AS department_name
FROM cleaned_names
GROUP BY 1, 2, 3, 4
