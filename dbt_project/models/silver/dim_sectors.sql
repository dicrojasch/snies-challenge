-- =============================================================================
-- Silver Dimension: dim_sectors
-- 3NF Entity: Sector of a Higher Education Institution (Public / Private)
-- Source IDs from SNIES are stable integers → used directly as PKs.
-- =============================================================================

WITH sectors_from_students AS (
    SELECT DISTINCT
        sector_id,
        sector_name
    FROM {{ ref('stg_students') }}
    WHERE sector_id IS NOT NULL
),

sectors_from_teachers AS (
    SELECT DISTINCT
        sector_id,
        sector_name
    FROM {{ ref('stg_teachers') }}
    WHERE sector_id IS NOT NULL
),

unioned AS (
    SELECT * FROM sectors_from_students
    UNION
    SELECT * FROM sectors_from_teachers
)

SELECT
    sector_id,
    -- Normalize casing to avoid duplication from inconsistent source data
    INITCAP(TRIM(sector_name)) AS sector_name
FROM unioned
