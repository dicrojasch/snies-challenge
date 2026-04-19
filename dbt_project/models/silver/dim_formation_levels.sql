-- =============================================================================
-- Silver Dimension: dim_formation_levels
-- 3NF Entity: Academic formation levels — unified across students and teachers.
-- students: nivel_de_formación  |  teachers: máximo_nivel_de_formación_del_docente
-- Both share the same SNIES numeric ID space, so they union cleanly.
-- =============================================================================

WITH from_students AS (
    SELECT DISTINCT
        formation_level_id,
        formation_level_name
    FROM {{ ref('stg_students') }}
    WHERE formation_level_id IS NOT NULL
),

from_teachers AS (
    SELECT DISTINCT
        formation_level_id,
        formation_level_name
    FROM {{ ref('stg_teachers') }}
    WHERE formation_level_id IS NOT NULL
),

unioned AS (
    SELECT * FROM from_students
    UNION
    SELECT * FROM from_teachers
)

SELECT
    formation_level_id,
    INITCAP(TRIM(formation_level_name)) AS formation_level_name
FROM unioned
