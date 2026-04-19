-- =============================================================================
-- Silver Dimension: dim_cine_classifications
-- 3NF Entity: UNESCO CINE / ISCED field-of-study hierarchy.
-- Modelled as a three-level hierarchy:
--   cine_detailed_field_id (PK, most granular)
--   → cine_specific_field_id  (mid level)
--   → cine_broad_field_id     (broad level, top of hierarchy)
-- Source: students only (teachers have only a text label, handled in teacher_records).
-- =============================================================================

SELECT DISTINCT
    cine_detailed_field_id,
    INITCAP(TRIM(cine_detailed_field_name)) AS cine_detailed_field_name,
    cine_specific_field_id,
    INITCAP(TRIM(cine_specific_field_name)) AS cine_specific_field_name,
    cine_broad_field_id,
    INITCAP(TRIM(cine_broad_field_name))    AS cine_broad_field_name
FROM {{ ref('stg_students') }}
WHERE cine_detailed_field_id IS NOT NULL
