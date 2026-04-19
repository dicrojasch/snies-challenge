-- =============================================================================
-- Staging: stg_teachers
-- Purpose : Cast, rename, and lightly clean the raw Bronze teachers table.
--           No business logic or aggregation — one row in, one row out.
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'docentes_2022') }}
),

renamed AS (
    SELECT
        -- ── Institution identity ──────────────────────────────────────────────
        CAST("código_de_la_institución"                        AS INTEGER) AS institution_id,
        CAST("ies_padre"                                       AS INTEGER) AS parent_institution_id,
        TRIM("institución_de_educación_superior_ies")                      AS institution_name,
        TRIM("principal_o_seccional")                                      AS branch_type,

        -- ── Sector ───────────────────────────────────────────────────────────
        CAST("id_sector_ies"                                   AS INTEGER) AS sector_id,
        TRIM("sector_ies")                                                 AS sector_name,

        -- ── Character (Carácter) ──────────────────────────────────────────────
        CAST("id_caracter"                                     AS INTEGER) AS character_id,
        TRIM("caracter_ies")                                               AS character_name,

        -- ── IES Geography ────────────────────────────────────────────────────
        CAST("código_del_departamento_ies"                     AS INTEGER) AS ies_department_id,
        TRIM("departamento_de_domicilio_de_la_ies")                        AS ies_department_name,
        CAST("código_del_municipio_ies"                        AS INTEGER) AS ies_municipality_id,
        TRIM("municipio_de_domicilio_de_la_ies")                           AS ies_municipality_name,

        -- ── Gender ────────────────────────────────────────────────────────────
        CAST("id_sexo"                                         AS INTEGER) AS gender_id,
        TRIM("sexo_del_docente")                                           AS gender_name,

        -- ── Formation Level ───────────────────────────────────────────────────
        CAST("id_máximo_nivel_de_formación_del_docente"        AS INTEGER) AS formation_level_id,
        TRIM("máximo_nivel_de_formación_del_docente")                      AS formation_level_name,

        -- ── CINE Level (text-only in teachers source, no numeric ID) ─────────
        CAST("nivel_cine"                                      AS INTEGER) AS cine_level_id,

        -- ── Dedication Type ───────────────────────────────────────────────────
        CAST("id_tiempo_de_dedicación"                         AS INTEGER) AS dedication_type_id,
        TRIM("tiempo_de_dedicación_del_docente")                           AS dedication_type_name,

        -- ── Contract Type ─────────────────────────────────────────────────────
        CAST("id_tipo_de_contrato"                             AS INTEGER) AS contract_type_id,
        TRIM("tipo_de_contrato_del_docente")                               AS contract_type_name,

        -- ── Time Dimension & Metric ───────────────────────────────────────────
        CAST("año"                                             AS INTEGER) AS data_year,
        CAST("semestre"                                        AS INTEGER) AS semester,
        CAST("no_de_docentes"                                  AS INTEGER) AS teacher_count,

        -- ── Audit ─────────────────────────────────────────────────────────────
        "loaded_at"                                                        AS loaded_at

    FROM source

    -- Keep only rows with a valid institution ID
    WHERE "código_de_la_institución" IS NOT NULL
)

SELECT * FROM renamed
