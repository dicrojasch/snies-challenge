-- =============================================================================
-- Silver Dimension: dim_contract_types
-- 3NF Entity: Teacher contract type (planta, ocasional, cátedra, etc.)
-- Source: teachers only.
-- =============================================================================

SELECT DISTINCT
    contract_type_id,
    INITCAP(TRIM(contract_type_name)) AS contract_type_name
FROM {{ ref('stg_teachers') }}
WHERE contract_type_id IS NOT NULL
