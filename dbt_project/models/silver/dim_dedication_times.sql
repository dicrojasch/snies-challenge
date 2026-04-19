-- =============================================================================
-- Silver Dimension: dim_dedication_times
-- 3NF Entity: Teacher dedication type (tiempo completo, medio tiempo, etc.)
-- Source: teachers only.
-- =============================================================================


-- models/silver/dim_dedication_times.sql

WITH raw_data AS (
    SELECT DISTINCT 
        dedication_time_id AS src_id, 
        dedication_time_name AS src_name
    FROM {{ ref('stg_teachers') }}
),

normalized AS (
    SELECT
        COALESCE(map.target_dedication_id, 0) AS dedication_time_id,
        COALESCE(map.target_dedication_name, 'Sin Información') AS dedication_time_name
    FROM raw_data
    LEFT JOIN {{ ref('map_dedication_time') }} AS map
        ON raw_data.src_id = map.source_dedication_id
        AND raw_data.src_name = map.source_dedication_name
)

SELECT DISTINCT
    dedication_time_id,
    INITCAP(TRIM(dedication_time_name)) AS dedication_time_name
FROM normalized
WHERE dedication_time_id IS NOT NULL
