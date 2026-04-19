-- =============================================================================
-- Silver Dimension: dim_institutions
-- 3NF Entity: Higher Education Institutions (IES).
-- PK  : institution_id (código_de_la_institución — stable SNIES integer)
-- FKs : sector_id, character_id, ies_municipality_id (geography),
--       parent_institution_id (self-referencing for SUE hierarchy)
-- =============================================================================

WITH base AS (
    SELECT DISTINCT ON (institution_id)
        institution_id,
        parent_institution_id,
        INITCAP(TRIM(institution_name))  AS institution_name,
        TRIM(branch_type)                AS branch_type,
        sector_id,
        character_id,
        ies_municipality_id              AS municipality_id
    FROM {{ ref('stg_students') }}
    WHERE institution_id IS NOT NULL
    ORDER BY institution_id, data_year DESC, semester DESC
),

from_teachers AS (
    SELECT DISTINCT ON (institution_id)
        institution_id,
        parent_institution_id,
        INITCAP(TRIM(institution_name))  AS institution_name,
        TRIM(branch_type)                AS branch_type,
        sector_id,
        character_id,
        ies_municipality_id              AS municipality_id
    FROM {{ ref('stg_teachers') }}
    WHERE institution_id IS NOT NULL
    ORDER BY institution_id, data_year DESC, semester DESC
),

-- Prefer student records; fall back to teacher records for IES that only
-- appear in teacher data (edge-case robustness).
combined AS (
    SELECT * FROM base
    UNION
    SELECT * FROM from_teachers
)

SELECT DISTINCT ON (institution_id)
    institution_id,
    parent_institution_id,
    institution_name,
    branch_type,
    sector_id,
    character_id,
    municipality_id,
    -- SUE membership: public IES whose parent_institution_id is not null
    -- belong to a state university system (SUE).
    CASE
        WHEN parent_institution_id IS NOT NULL AND sector_id = 1 THEN TRUE
        ELSE FALSE
    END AS is_sue_member
FROM combined
ORDER BY institution_id
