-- =============================================================================
-- Silver Dimension: dim_dedication_types
-- 3NF Entity: Teacher dedication type (tiempo completo, medio tiempo, etc.)
-- Source: teachers only.
-- =============================================================================

SELECT DISTINCT
    dedication_type_id,
    INITCAP(TRIM(dedication_type_name)) AS dedication_type_name
FROM {{ ref('stg_teachers') }}
WHERE dedication_type_id IS NOT NULL
