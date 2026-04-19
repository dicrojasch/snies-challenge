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

WITH base AS (
    SELECT DISTINCT ON (program_id)
        program_id,
        INITCAP(TRIM(program_name))         AS program_name,

        -- ── Methodology ───────────────────────────────────────────────────────
        methodology_id,
        INITCAP(TRIM(methodology_name))     AS methodology_name,

        -- ── Knowledge Hierarchy ───────────────────────────────────────────────
        knowledge_area_id,
        INITCAP(TRIM(knowledge_area_name))  AS knowledge_area_name,
        nbc_id,
        INITCAP(TRIM(nbc_name))             AS nbc_name,

        -- ── FKs to normalized dimensions ─────────────────────────────────────
        academic_level_id,
        formation_level_id,
        cine_detailed_field_id,

        -- ── Primary offer municipality (from programme columns) ───────────────
        program_municipality_id             AS offer_municipality_id,

        -- ── SUE flag: institutions in SUE belong to public sector (id=1) ─────
        -- We keep institution_id here so the union with dim_institutions is clean
        institution_id                      AS ies_code

    FROM {{ ref('stg_students') }}
    WHERE program_id IS NOT NULL
    ORDER BY program_id, data_year DESC, semester DESC
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
FROM base
