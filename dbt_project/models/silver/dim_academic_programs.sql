-- =============================================================================
-- Silver Dimension: dim_academic_programs
-- 3NF Entity: Academic programmes offered by IES.
-- PK  : program_id (código_snies_del_programa — stable SNIES integer)
-- FKs : academic_level_id, formation_level_id, methodology_id,
--       knowledge_area_id, nbc_id, cine_detailed_field_id,
--       ies_municipality_id (offer geography).
-- One programme can be offered at multiple municipalities; the lowest
-- granularity from the source is (programme × municipality).  We model
-- the programme with its PRIMARY offer municipality as a single entity.
-- =============================================================================

-- models/silver/dim_academic_programs.sql

WITH base AS (
    SELECT DISTINCT ON (program_id)
        program_id,
        INITCAP(TRIM(program_name))         AS program_name,

        -- Methodology
        methodology_id,
        INITCAP(TRIM(methodology_name))     AS methodology_name,

        -- Knowledge Hierarchy
        knowledge_area_id,
        INITCAP(TRIM(knowledge_area_name))  AS knowledge_area_name,
        nbc_id,
        INITCAP(TRIM(nbc_name))             AS nbc_name,

        -- Raw fields needed for mapping/joining seeds
        academic_level_id,
        formation_level_id                  AS src_formation_id,
        formation_level_name                AS src_formation_name,
        cine_detailed_field_id              AS src_cine_id,
        program_municipality_id             AS src_municipality_id,
        institution_id                      AS ies_code

    FROM {{ ref('stg_students') }}
    WHERE program_id IS NOT NULL
    ORDER BY program_id, data_year DESC, semester DESC
),

mapped AS (
    SELECT
        b.program_id,
        b.program_name,
        b.methodology_id,
        b.methodology_name,
        b.knowledge_area_id,
        b.knowledge_area_name,
        b.nbc_id,
        b.nbc_name,
        b.academic_level_id,
        b.ies_code,

        -- 1. Map Formation Level ID (Using same logic as dim_formation_levels)
        COALESCE(fl.target_id, b.src_formation_id, 0) AS formation_level_id,

        -- 2. Map CINE Field ID
        COALESCE(c.detailed_id, b.src_cine_id, 0)      AS cine_detailed_field_id,

        -- 3. Map Geography ID
        COALESCE(g.municipality_id, b.src_municipality_id, 0) AS offer_municipality_id

    FROM base b
    LEFT JOIN {{ ref('map_formation_levels') }} fl
        ON b.src_formation_id = fl.src_id
        AND INITCAP(TRIM(b.src_formation_name)) = INITCAP(TRIM(fl.src_name))
    
    LEFT JOIN {{ ref('map_cine_fields') }} c
        ON b.src_cine_id = c.detailed_id

    LEFT JOIN {{ ref('map_geography') }} g
        ON b.src_municipality_id = g.municipality_id
)

SELECT
    program_id,
    program_name,
    methodology_id,
    methodology_name,
    knowledge_area_id,
    knowledge_area_name,
    nbc_id,
    nbc_name,
    academic_level_id,
    formation_level_id,
    cine_detailed_field_id,
    offer_municipality_id,
    ies_code
FROM mapped
