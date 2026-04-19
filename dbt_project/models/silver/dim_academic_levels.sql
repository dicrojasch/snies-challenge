-- =============================================================================
-- Silver Dimension: dim_academic_levels
-- 3NF Entity: Academic levels (pregrado, posgrado, etc.)
-- Source: students only.
-- =============================================================================

SELECT DISTINCT
    academic_level_id,
    INITCAP(TRIM(academic_level_name)) AS academic_level_name
FROM {{ ref('stg_students') }}
WHERE academic_level_id IS NOT NULL
