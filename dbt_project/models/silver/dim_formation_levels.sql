-- =============================================================================
-- Silver Dimension: dim_formation_levels
-- 3NF Entity: Academic formation levels — unified across students and teachers.
-- students: nivel_de_formación  |  teachers: máximo_nivel_de_formación_del_docente
-- Both share the same SNIES numeric ID space, so they union cleanly.
-- =============================================================================
-- models/silver/dim_formation_levels.sql

WITH from_students AS (
    SELECT DISTINCT
        formation_level_id AS src_id,
        formation_level_name AS src_name
    FROM {{ ref('stg_students') }}
),

from_teachers AS (
    SELECT DISTINCT
        formation_level_id AS src_id,
        formation_level_name AS src_name
    FROM {{ ref('stg_teachers') }}
),

unioned AS (
    SELECT * FROM from_students
    UNION
    SELECT * FROM from_teachers
),

mapped AS (
    SELECT
        -- If the combination exists in our seed, use the target; otherwise keep original or 0
        COALESCE(map.target_id, 0) AS formation_level_id,
        COALESCE(map.target_name, 'No Definido') AS formation_level_name
    FROM unioned AS u
    LEFT JOIN {{ ref('map_formation_levels') }} AS map
        ON u.src_id = map.src_id
        -- We use TRIM and INITCAP to ensure the join matches correctly
        AND INITCAP(TRIM(u.src_name)) = INITCAP(TRIM(map.src_name))
)

-- Final distinct to clean up any overlaps after mapping
SELECT DISTINCT
    formation_level_id,
    formation_level_name
FROM mapped
ORDER BY formation_level_id
