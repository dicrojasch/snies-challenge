-- =============================================================================
-- Staging: stg_enrollment
-- Purpose : Cast, rename, and lightly clean the raw Bronze enrollment table.
-- =============================================================================

WITH source_2022 AS (
    SELECT * FROM {{ source('bronze', 'estudiantes_matriculados_2022') }}
),

source_2023 AS (
    SELECT * FROM {{ source('bronze', 'estudiantes_matriculados_2023') }}
),

source_2024 AS (
    SELECT * FROM {{ source('bronze', 'estudiantes_matriculados_2024') }}
),

renamed AS (
    -- =============================================================================
-- Staging: stg_enrollment_2022
-- Purpose : Cast, rename, and lightly clean the raw Bronze enrollment table.
-- =============================================================================
    SELECT
        -- ── Institution identity ──────────────────────────────────────────────
        CASE 
            WHEN CAST("código_de_la_institución" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_de_la_institución" AS INTEGER)
            ELSE NULL 
        END                                                                      AS institution_id,
        CASE 
            WHEN CAST("ies_padre" AS TEXT) ~ '^[0-9]+$' THEN CAST("ies_padre" AS INTEGER)
            ELSE NULL 
        END                                                                      AS parent_institution_id,
        TRIM("institución_de_educación_superior_ies")                               AS institution_name,
        TRIM("principal_o_seccional")                                               AS institution_type,
        TRIM("ies_acreditada")                                                       AS is_institution_accredited,

        -- ── Sector ───────────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_sector_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_sector_ies" AS INTEGER)
            ELSE NULL 
        END                                                                      AS sector_id,
        TRIM("sector_ies")                                                          AS sector_name,

        -- ── Character (Carácter) ──────────────────────────────────────────────
        CASE 
            WHEN CAST("id_caracter" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_caracter" AS INTEGER)
            ELSE NULL 
        END                                                                      AS character_id,
        TRIM("caracter_ies")                                                        AS character_name,

        -- ── IES Geography ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("código_del_departamento_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_departamento_ies" AS INTEGER)
            ELSE NULL 
        END                                                                      AS ies_department_id,
        TRIM("departamento_de_domicilio_de_la_ies")                                 AS ies_department_name,
        CASE 
            WHEN CAST("código_del_municipio_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_municipio_ies" AS INTEGER)
            ELSE NULL 
        END                                                                      AS ies_municipality_id,
        TRIM("municipio_de_domicilio_de_la_ies")                                    AS ies_municipality_name,

        -- ── Academic Programme ────────────────────────────────────────────────
        CASE 
            WHEN CAST("código_snies_del_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_snies_del_programa" AS INTEGER)
            ELSE NULL 
        END                                                                      AS program_id,
        TRIM("programa_académico")                                                  AS program_name,
        TRIM("programa_acreditado")                                                 AS is_program_accredited,

        -- ── Academic Level ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_nivel_académico" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_nivel_académico" AS INTEGER)
            ELSE NULL 
        END                                                                      AS academic_level_id,
        TRIM("nivel_académico")                                                     AS academic_level_name,

        -- ── Formation Level ───────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_nivel_de_formación" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_nivel_de_formación" AS INTEGER)
            ELSE NULL 
        END                                                                      AS formation_level_id,
        TRIM("nivel_de_formación")                                                  AS formation_level_name,

        -- ── Methodology ───────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_metodología" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_metodología" AS INTEGER)
            ELSE NULL 
        END                                                                      AS methodology_id,
        TRIM("metodología")                                                         AS methodology_name,

        -- ── Knowledge Area ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_área_de_conocimiento" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_área_de_conocimiento" AS INTEGER)
            ELSE NULL 
        END                                                                      AS knowledge_area_id,
        TRIM("área_de_conocimiento")                                                AS knowledge_area_name,

        -- ── NBC (Núcleo Básico del Conocimiento) ──────────────────────────────
        CASE 
            WHEN CAST("id_núcleo" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_núcleo" AS INTEGER)
            ELSE NULL 
        END                                                                      AS nbc_id,
        TRIM("núcleo_básico_del_conocimiento_nbc")                                  AS nbc_name,

        -- ── CINE Classification ───────────────────────────────────────────────
        CASE 
            WHEN CAST("id_cine_campo_amplio" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_amplio" AS INTEGER)
            ELSE NULL 
        END                                                                      AS cine_broad_field_id,
        TRIM("desc_cine_campo_amplio")                                              AS cine_broad_field_name,
        CASE 
            WHEN CAST("id_cine_campo_especifico" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_especifico" AS INTEGER)
            ELSE NULL 
        END                                                                      AS cine_specific_field_id,
        TRIM("desc_cine_campo_especifico")                                          AS cine_specific_field_name,
        CASE 
            WHEN CAST("id_cine_codigo_detallado" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_codigo_detallado" AS INTEGER)
            ELSE NULL 
        END                                                                      AS cine_detailed_field_id,
        TRIM("desc_cine_codigo_detallado")                                          AS cine_detailed_field_name,

        -- ── Programme Offer Geography ─────────────────────────────────────────
        CASE 
            WHEN CAST("código_del_departamento_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_departamento_programa" AS INTEGER)
            ELSE NULL 
        END                                                                      AS program_department_id,
        TRIM("departamento_de_oferta_del_programa")                                 AS program_department_name,
        CASE 
            WHEN CAST("código_del_municipio_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_municipio_programa" AS INTEGER)
            ELSE NULL 
        END                                                                      AS program_municipality_id,
        TRIM("municipio_de_oferta_del_programa")                                    AS program_municipality_name,

        -- ── Gender ────────────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_sexo" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_sexo" AS INTEGER)
            ELSE NULL 
        END                                                                      AS gender_id,
        TRIM("sexo")                                                                AS gender_name,

        -- ── Time Dimension & Metric ───────────────────────────────────────────
        CASE 
            WHEN CAST("año" AS TEXT) ~ '^[0-9]+$' THEN CAST("año" AS INTEGER)
            ELSE NULL 
        END                                                                      AS data_year,
        CASE 
            WHEN CAST("semestre" AS TEXT) ~ '^[0-9]+$' THEN CAST("semestre" AS INTEGER)
            ELSE NULL 
        END                                                                      AS semester,
        CASE 
            WHEN CAST("matriculados" AS TEXT) ~ '^[0-9]+$' THEN CAST("matriculados" AS INTEGER)
            ELSE NULL 
        END                                                                      AS enrollment_count,

        -- ── Audit ─────────────────────────────────────────────────────────────
        "loaded_at"                                                                 AS loaded_at

    FROM source_2022

    UNION ALL
-- =============================================================================
-- Staging: stg_enrollment_2023
-- Purpose : Cast, rename, and lightly clean the raw Bronze enrollment table (2023).
-- =============================================================================
    SELECT
        -- ── Institution identity ──────────────────────────────────────────────
        CASE 
            WHEN CAST("código_de_la_institución" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_de_la_institución" AS INTEGER)
            ELSE NULL 
        END                                                                      AS institution_id,
        CASE 
            WHEN CAST("ies_padre" AS TEXT) ~ '^[0-9]+$' THEN CAST("ies_padre" AS INTEGER)
            ELSE NULL 
        END                                                                      AS parent_institution_id,
        TRIM("institución_de_educación_superior_ies")                               AS institution_name,
        TRIM("tipo_ies")                                                            AS institution_type,
        TRIM("ies_acreditada")                                                       AS is_institution_accredited,

        -- ── Sector ───────────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_sector_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_sector_ies" AS INTEGER)
            ELSE NULL 
        END                                                                      AS sector_id,
        TRIM("sector_ies")                                                          AS sector_name,

        -- ── Character (Carácter) ──────────────────────────────────────────────
        CASE 
            WHEN CAST("id_carácter_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_carácter_ies" AS INTEGER)
            ELSE NULL 
        END                                                                      AS character_id,
        TRIM("carácter_ies")                                                        AS character_name,

        -- ── IES Geography ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("código_del_departamento_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_departamento_ies" AS INTEGER)
            ELSE NULL 
        END                                                                      AS ies_department_id,
        TRIM("departamento_de_domicilio_de_la_ies")                                 AS ies_department_name,
        CASE 
            WHEN CAST("código_del_municipio_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_municipio_ies" AS INTEGER)
            ELSE NULL 
        END                                                                      AS ies_municipality_id,
        TRIM("municipio_de_domicilio_de_la_ies")                                    AS ies_municipality_name,

        -- ── Academic Programme ────────────────────────────────────────────────
        CASE 
            WHEN CAST("código_snies_del_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_snies_del_programa" AS INTEGER)
            ELSE NULL 
        END                                                                      AS program_id,
        TRIM("programa_académico")                                                  AS program_name,
        TRIM("programa_acreditado")                                                 AS is_program_accredited,

        -- ── Academic Level ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_nivel_académico" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_nivel_académico" AS INTEGER)
            ELSE NULL 
        END                                                                      AS academic_level_id,
        TRIM("nivel_académico")                                                     AS academic_level_name,

        -- ── Formation Level ───────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_nivel_de_formación" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_nivel_de_formación" AS INTEGER)
            ELSE NULL 
        END                                                                      AS formation_level_id,
        TRIM("nivel_de_formación")                                                  AS formation_level_name,

        -- ── Modality (Methodology) ───────────────────────────────────────────
        CASE 
            WHEN CAST("id_modalidad" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_modalidad" AS INTEGER)
            ELSE NULL 
        END                                                                      AS methodology_id,
        TRIM("modalidad")                                                           AS methodology_name,

        -- ── Knowledge Area ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_área" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_área" AS INTEGER)
            ELSE NULL 
        END                                                                      AS knowledge_area_id,
        TRIM("área_de_conocimiento")                                                AS knowledge_area_name,

        -- ── NBC (Núcleo Básico del Conocimiento) ──────────────────────────────
        CASE 
            WHEN CAST("id_núcleo" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_núcleo" AS INTEGER)
            ELSE NULL 
        END                                                                      AS nbc_id,
        TRIM("núcleo_básico_del_conocimiento_nbc")                                  AS nbc_name,

        -- ── CINE Classification ───────────────────────────────────────────────
        CASE 
            WHEN CAST("id_cine_campo_amplio" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_amplio" AS INTEGER)
            ELSE NULL 
        END                                                                      AS cine_broad_field_id,
        TRIM("desc_cine_campo_amplio")                                              AS cine_broad_field_name,
        CASE 
            WHEN CAST("id_cine_campo_especifico" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_especifico" AS INTEGER)
            ELSE NULL 
        END                                                                      AS cine_specific_field_id,
        TRIM("desc_cine_campo_especifico")                                          AS cine_specific_field_name,
        CASE 
            WHEN CAST("id_cine_campo_detallado" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_detallado" AS INTEGER)
            ELSE NULL 
        END                                                                      AS cine_detailed_field_id,
        TRIM("desc_cine_campo_detallado")                                           AS cine_detailed_field_name,

        -- ── Programme Offer Geography ─────────────────────────────────────────
        CASE 
            WHEN CAST("código_del_departamento_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_departamento_programa" AS INTEGER)
            ELSE NULL 
        END                                                                      AS program_department_id,
        TRIM("departamento_de_oferta_del_programa")                                 AS program_department_name,
        CASE 
            WHEN CAST("código_del_municipio_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_municipio_programa" AS INTEGER)
            ELSE NULL 
        END                                                                      AS program_municipality_id,
        TRIM("municipio_de_oferta_del_programa")                                    AS program_municipality_name,

        -- ── Gender ────────────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_sexo" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_sexo" AS INTEGER)
            ELSE NULL 
        END                                                                      AS gender_id,
        TRIM("sexo")                                                                AS gender_name,

        -- ── Time Dimension & Metric ───────────────────────────────────────────
        CASE 
            WHEN CAST("año" AS TEXT) ~ '^[0-9]+$' THEN CAST("año" AS INTEGER)
            ELSE NULL 
        END                                                                      AS data_year,
        CASE 
            WHEN CAST("semestre" AS TEXT) ~ '^[0-9]+$' THEN CAST("semestre" AS INTEGER)
            ELSE NULL 
        END                                                                      AS semester,
        CASE 
            WHEN CAST("matriculados" AS TEXT) ~ '^[0-9]+$' THEN CAST("matriculados" AS INTEGER)
            ELSE NULL 
        END                                                                      AS enrollment_count,

        -- ── Audit ─────────────────────────────────────────────────────────────
        "loaded_at"                                                                 AS loaded_at

    FROM source_2023

    UNION ALL
-- =============================================================================
-- Staging: stg_enrollment_2024
-- Purpose : Cast, rename, and lightly clean the raw Bronze enrollment table (2024).
-- =============================================================================
    SELECT
        -- ── Institution identity ──────────────────────────────────────────────
        CASE 
            WHEN CAST("código_de_la_institución" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_de_la_institución" AS INTEGER)
            ELSE NULL 
        END                                                                      AS institution_id,
        CASE 
            WHEN CAST("ies_padre" AS TEXT) ~ '^[0-9]+$' THEN CAST("ies_padre" AS INTEGER)
            ELSE NULL 
        END                                                                      AS parent_institution_id,
        TRIM("institución_de_educación_superior_ies")                               AS institution_name,
        TRIM("tipo_ies")                                                            AS institution_type,
        TRIM("ies_acreditada")                                                       AS is_institution_accredited,

        -- ── Sector ───────────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_sector_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_sector_ies" AS INTEGER)
            ELSE NULL 
        END                                                                      AS sector_id,
        TRIM("sector_ies")                                                          AS sector_name,

        -- ── Character (Carácter) ──────────────────────────────────────────────
        CASE 
            WHEN CAST("id_carácter_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_carácter_ies" AS INTEGER)
            ELSE NULL 
        END                                                                      AS character_id,
        TRIM("carácter_ies")                                                        AS character_name,

        -- ── IES Geography ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("código_del_departamento_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_departamento_ies" AS INTEGER)
            ELSE NULL 
        END                                                                      AS ies_department_id,
        TRIM("departamento_de_domicilio_de_la_ies")                                 AS ies_department_name,
        CASE 
            WHEN CAST("código_del_municipio_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_municipio_ies" AS INTEGER)
            ELSE NULL 
        END                                                                      AS ies_municipality_id,
        TRIM("municipio_de_domicilio_de_la_ies")                                    AS ies_municipality_name,

        -- ── Academic Programme ────────────────────────────────────────────────
        CASE 
            WHEN CAST("código_snies_del_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_snies_del_programa" AS INTEGER)
            ELSE NULL 
        END                                                                      AS program_id,
        TRIM("programa_académico")                                                  AS program_name,
        TRIM("programa_acreditado")                                                 AS is_program_accredited,

        -- ── Academic Level ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_nivel_académico" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_nivel_académico" AS INTEGER)
            ELSE NULL 
        END                                                                      AS academic_level_id,
        TRIM("nivel_académico")                                                     AS academic_level_name,

        -- ── Formation Level ───────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_nivel_de_formación" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_nivel_de_formación" AS INTEGER)
            ELSE NULL 
        END                                                                      AS formation_level_id,
        TRIM("nivel_de_formación")                                                  AS formation_level_name,

        -- ── Modality (Methodology) ───────────────────────────────────────────
        CASE 
            WHEN CAST("id_modalidad" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_modalidad" AS INTEGER)
            ELSE NULL 
        END                                                                      AS methodology_id,
        TRIM("modalidad")                                                           AS methodology_name,

        -- ── Knowledge Area ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_área" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_área" AS INTEGER)
            ELSE NULL 
        END                                                                      AS knowledge_area_id,
        TRIM("área_de_conocimiento")                                                AS knowledge_area_name,

        -- ── NBC (Núcleo Básico del Conocimiento) ──────────────────────────────
        CASE 
            WHEN CAST("id_núcleo" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_núcleo" AS INTEGER)
            ELSE NULL 
        END                                                                      AS nbc_id,
        TRIM("núcleo_básico_del_conocimiento_nbc")                                  AS nbc_name,

        -- ── CINE Classification ───────────────────────────────────────────────
        CASE 
            WHEN CAST("id_cine_campo_amplio" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_amplio" AS INTEGER)
            ELSE NULL 
        END                                                                      AS cine_broad_field_id,
        TRIM("desc_cine_campo_amplio")                                              AS cine_broad_field_name,
        CASE 
            WHEN CAST("id_cine_campo_especifico" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_especifico" AS INTEGER)
            ELSE NULL 
        END                                                                      AS cine_specific_field_id,
        TRIM("desc_cine_campo_especifico")                                          AS cine_specific_field_name,
        CASE 
            WHEN CAST("id_cine_campo_detallado" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_detallado" AS INTEGER)
            ELSE NULL 
        END                                                                      AS cine_detailed_field_id,
        TRIM("desc_cine_campo_detallado")                                           AS cine_detailed_field_name,

        -- ── Programme Offer Geography ─────────────────────────────────────────
        CASE 
            WHEN CAST("código_del_departamento_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_departamento_programa" AS INTEGER)
            ELSE NULL 
        END                                                                      AS program_department_id,
        TRIM("departamento_de_oferta_del_programa")                                 AS program_department_name,
        CASE 
            WHEN CAST("código_del_municipio_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_municipio_programa" AS INTEGER)
            ELSE NULL 
        END                                                                      AS program_municipality_id,
        TRIM("municipio_de_oferta_del_programa")                                    AS program_municipality_name,

        -- ── Gender ────────────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_sexo" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_sexo" AS INTEGER)
            ELSE NULL 
        END                                                                      AS gender_id,
        TRIM("sexo")                                                                AS gender_name,

        -- ── Time Dimension & Metric ───────────────────────────────────────────
        CASE 
            WHEN CAST("año" AS TEXT) ~ '^[0-9]+$' THEN CAST("año" AS INTEGER)
            ELSE NULL 
        END                                                                      AS data_year,
        CASE 
            WHEN CAST("semestre" AS TEXT) ~ '^[0-9]+$' THEN CAST("semestre" AS INTEGER)
            ELSE NULL 
        END                                                                      AS semester,
        CASE 
            WHEN CAST("matriculados" AS TEXT) ~ '^[0-9]+$' THEN CAST("matriculados" AS INTEGER)
            ELSE NULL 
        END                                                                      AS enrollment_count,

        -- ── Audit ─────────────────────────────────────────────────────────────
        "loaded_at"                                                                 AS loaded_at

    FROM source_2024
)

SELECT * FROM renamed