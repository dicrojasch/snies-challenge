-- =============================================================================
-- Silver Dimension: dim_sectors
-- 3NF Entity: Sector of a Higher Education Institution (Public / Private)
-- Source IDs from SNIES are stable integers → used directly as PKs.
-- =============================================================================

WITH sectors_from_students AS (
    SELECT DISTINCT
        sector_id AS src_id,
        sector_name AS src_name
    FROM {{ ref('stg_students') }}
    WHERE sector_id IS NOT NULL
),

sectors_from_teachers AS (
    SELECT DISTINCT
        sector_id AS src_id,
        sector_name AS src_name
    FROM {{ ref('stg_teachers') }}
    WHERE sector_id IS NOT NULL
),

unioned AS (
    SELECT * FROM sectors_from_students
    UNION
    SELECT * FROM sectors_from_teachers
),

mapped AS (
    SELECT
        COALESCE(m.target_id, u.src_id) AS sector_id,
        -- Agregamos un COALESCE adicional para evitar nulos finales
        COALESCE(
            m.target_name, 
            NULLIF(INITCAP(TRIM(u.src_name)), ''), -- Convierte string vacío en NULL
            'No Definido'                         -- Si todo falla, pone este nombre
        ) AS sector_name
    FROM unioned u
    LEFT JOIN {{ ref('map_sectors') }} m
        ON u.src_id = m.src_id
        AND INITCAP(TRIM(u.src_name)) = INITCAP(TRIM(m.src_name))
)

-- Final SELECT DISTINCT to merge the "Privado" and "Privada" rows into one
SELECT DISTINCT
    sector_id,
    sector_name
FROM mapped
ORDER BY sector_id
