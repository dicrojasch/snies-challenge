-- =============================================================================
-- Silver Dimension: dim_characters
-- 3NF Entity: Institutional character (Universidad, Tecnológica, etc.)
-- Source IDs from SNIES are stable integers → used directly as PKs.
-- =============================================================================

WITH chars_from_students AS (
    SELECT DISTINCT
        character_id,
        character_name
    FROM {{ ref('stg_students') }}
    WHERE character_id IS NOT NULL
),

chars_from_teachers AS (
    SELECT DISTINCT
        character_id,
        character_name
    FROM {{ ref('stg_teachers') }}
    WHERE character_id IS NOT NULL
),

unioned AS (
    SELECT * FROM chars_from_students
    UNION
    SELECT * FROM chars_from_teachers
)

SELECT
    character_id,
    INITCAP(TRIM(character_name)) AS character_name
FROM unioned
