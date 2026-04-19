-- =============================================================================
-- Silver Dimension: dim_dedication_times
-- 3NF Entity: Teacher dedication type (tiempo completo, medio tiempo, etc.)
-- Source: teachers only.
-- =============================================================================

SELECT DISTINCT
    dedication_time_id,
    INITCAP(TRIM(dedication_time_name)) AS dedication_time_name
FROM {{ ref('stg_teachers') }}
WHERE dedication_time_id IS NOT NULL
