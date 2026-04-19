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
            ELSE -1 
        END                                                                      AS institution_id,
        CASE 
            WHEN CAST("ies_padre" AS TEXT) ~ '^[0-9]+$' THEN CAST("ies_padre" AS INTEGER)
            ELSE NULL 
        END                                                                      AS parent_institution_id,
        CASE 
            WHEN TRIM("institución_de_educación_superior_ies") = '' OR "institución_de_educación_superior_ies" IS NULL THEN 'Unknown'
            ELSE TRIM("institución_de_educación_superior_ies")
        END                                                                      AS institution_name,
        TRIM("principal_o_seccional")                                               AS institution_type,
        TRIM("ies_acreditada")                                                       AS is_institution_accredited,

        -- ── Sector ───────────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_sector_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_sector_ies" AS INTEGER)
            ELSE -1 
        END                                                                      AS sector_id,
        TRIM("sector_ies")                                                          AS sector_name,

        -- ── Character (Carácter) ──────────────────────────────────────────────
        CASE 
            WHEN CAST("id_caracter" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_caracter" AS INTEGER)
            ELSE -1 
        END                                                                      AS character_id,
        CASE 
            WHEN TRIM("caracter_ies") = '' OR "caracter_ies" IS NULL THEN 'Unknown'
            ELSE TRIM("caracter_ies")
        END                                                                      AS character_name,

        -- ── IES Geography ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("código_del_departamento_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_departamento_ies" AS INTEGER)
            ELSE -1 
        END                                                                      AS ies_department_id,
        CASE 
            WHEN TRIM("departamento_de_domicilio_de_la_ies") = '' OR "departamento_de_domicilio_de_la_ies" IS NULL THEN 'Unknown'
            ELSE TRIM("departamento_de_domicilio_de_la_ies")
        END                                                                      AS ies_department_name,
        CASE 
            WHEN CAST("código_del_municipio_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_municipio_ies" AS INTEGER)
            ELSE -1 
        END                                                                      AS ies_municipality_id,
        CASE 
            WHEN TRIM("municipio_de_domicilio_de_la_ies") = '' OR "municipio_de_domicilio_de_la_ies" IS NULL THEN 'Unknown'
            ELSE TRIM("municipio_de_domicilio_de_la_ies")
        END                                                                      AS ies_municipality_name,

        -- ── Academic Programme ────────────────────────────────────────────────
        CASE 
            WHEN CAST("código_snies_del_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_snies_del_programa" AS INTEGER)
            ELSE -1 
        END                                                                      AS program_id,
        CASE 
            WHEN TRIM("programa_académico") = '' OR "programa_académico" IS NULL THEN 'Unknown'
            ELSE TRIM("programa_académico")
        END                                                                      AS program_name,
        TRIM("programa_acreditado")                                                 AS is_program_accredited,

        -- ── Academic Level ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_nivel_académico" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_nivel_académico" AS INTEGER)
            ELSE -1 
        END                                                                      AS academic_level_id,
        CASE 
            WHEN TRIM("nivel_académico") = '' OR "nivel_académico" IS NULL THEN 'Unknown'
            ELSE TRIM("nivel_académico")
        END                                                                      AS academic_level_name,

        -- ── Formation Level ───────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_nivel_de_formación" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_nivel_de_formación" AS INTEGER)
            ELSE -1 
        END                                                                      AS formation_level_id,
        CASE 
            WHEN TRIM("nivel_de_formación") = '' OR "nivel_de_formación" IS NULL THEN 'Unknown'
            ELSE TRIM("nivel_de_formación")
        END                                                                      AS formation_level_name,

        -- ── Methodology ───────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_metodología" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_metodología" AS INTEGER)
            ELSE -1 
        END                                                                      AS methodology_id,
        TRIM("metodología")                                                         AS methodology_name,

        -- ── Knowledge Area ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_área_de_conocimiento" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_área_de_conocimiento" AS INTEGER)
            ELSE -1 
        END                                                                      AS knowledge_area_id,
        TRIM("área_de_conocimiento")                                                AS knowledge_area_name,

        -- ── NBC (Núcleo Básico del Conocimiento) ──────────────────────────────
        CASE 
            WHEN CAST("id_núcleo" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_núcleo" AS INTEGER)
            ELSE -1 
        END                                                                      AS nbc_id,
        TRIM("núcleo_básico_del_conocimiento_nbc")                                  AS nbc_name,

        -- ── CINE Classification ───────────────────────────────────────────────
        CASE 
            WHEN CAST("id_cine_campo_amplio" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_amplio" AS INTEGER)
            ELSE -1 
        END                                                                      AS cine_broad_field_id,
        CASE 
            WHEN TRIM("desc_cine_campo_amplio") = '' OR "desc_cine_campo_amplio" IS NULL THEN 'Unknown'
            ELSE TRIM("desc_cine_campo_amplio")
        END                                                                      AS cine_broad_field_name,
        CASE 
            WHEN CAST("id_cine_campo_especifico" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_especifico" AS INTEGER)
            ELSE -1 
        END                                                                      AS cine_specific_field_id,
        CASE 
            WHEN TRIM("desc_cine_campo_especifico") = '' OR "desc_cine_campo_especifico" IS NULL THEN 'Unknown'
            ELSE TRIM("desc_cine_campo_especifico")
        END                                                                      AS cine_specific_field_name,
        CASE 
            WHEN CAST("id_cine_codigo_detallado" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_codigo_detallado" AS INTEGER)
            ELSE -1 
        END                                                                      AS cine_detailed_field_id,
        CASE 
            WHEN TRIM("desc_cine_codigo_detallado") = '' OR "desc_cine_codigo_detallado" IS NULL THEN 'Unknown'
            ELSE TRIM("desc_cine_codigo_detallado")
        END                                                                      AS cine_detailed_field_name,

        -- ── Programme Offer Geography ─────────────────────────────────────────
        CASE 
            WHEN CAST("código_del_departamento_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_departamento_programa" AS INTEGER)
            ELSE -1 
        END                                                                      AS program_department_id,
        CASE 
            WHEN TRIM("departamento_de_oferta_del_programa") = '' OR "departamento_de_oferta_del_programa" IS NULL THEN 'Unknown'
            ELSE TRIM("departamento_de_oferta_del_programa")
        END                                                                      AS program_department_name,
        CASE 
            WHEN CAST("código_del_municipio_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_municipio_programa" AS INTEGER)
            ELSE -1 
        END                                                                      AS program_municipality_id,
        CASE 
            WHEN TRIM("municipio_de_oferta_del_programa") = '' OR "municipio_de_oferta_del_programa" IS NULL THEN 'Unknown'
            ELSE TRIM("municipio_de_oferta_del_programa")
        END                                                                      AS program_municipality_name,

        -- ── Gender ────────────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_sexo" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_sexo" AS INTEGER)
            ELSE -1
        END                                                                      AS gender_id,
        CASE 
            WHEN TRIM("sexo") = '' OR "sexo" IS NULL THEN 'Unknown'
            ELSE TRIM("sexo")
        END                                                                      AS gender_name,

        -- ── Time Dimension & Metric ───────────────────────────────────────────
        CASE 
            WHEN CAST("año" AS TEXT) ~ '^[0-9]+$' THEN CAST("año" AS INTEGER)
            ELSE -1
        END                                                                      AS data_year,
        CASE 
            WHEN CAST("semestre" AS TEXT) ~ '^[0-9]+$' THEN CAST("semestre" AS INTEGER)
            ELSE -1
        END                                                                      AS semester,
        CASE 
            WHEN CAST("matriculados" AS TEXT) ~ '^[0-9]+$' THEN CAST("matriculados" AS INTEGER)
            ELSE 0 
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
            ELSE -1 
        END                                                                      AS institution_id,
        CASE 
            WHEN CAST("ies_padre" AS TEXT) ~ '^[0-9]+$' THEN CAST("ies_padre" AS INTEGER)
            ELSE NULL 
        END                                                                      AS parent_institution_id,
        CASE 
            WHEN TRIM("institución_de_educación_superior_ies") = '' OR "institución_de_educación_superior_ies" IS NULL THEN 'Unknown'
            ELSE TRIM("institución_de_educación_superior_ies")
        END                                                                      AS institution_name,
        TRIM("tipo_ies")                                                            AS institution_type,
        TRIM("ies_acreditada")                                                       AS is_institution_accredited,

        -- ── Sector ───────────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_sector_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_sector_ies" AS INTEGER)
            ELSE -1 
        END                                                                      AS sector_id,
        TRIM("sector_ies")                                                          AS sector_name,

        -- ── Character (Carácter) ──────────────────────────────────────────────
        CASE 
            WHEN CAST("id_carácter_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_carácter_ies" AS INTEGER)
            ELSE -1 
        END                                                                      AS character_id,
        CASE 
            WHEN TRIM("carácter_ies") = '' OR "carácter_ies" IS NULL THEN 'Unknown'
            ELSE TRIM("carácter_ies")
        END                                                                      AS character_name,

        -- ── IES Geography ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("código_del_departamento_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_departamento_ies" AS INTEGER)
            ELSE -1 
        END                                                                      AS ies_department_id,
        CASE 
            WHEN TRIM("departamento_de_domicilio_de_la_ies") = '' OR "departamento_de_domicilio_de_la_ies" IS NULL THEN 'Unknown'
            ELSE TRIM("departamento_de_domicilio_de_la_ies")
        END                                                                      AS ies_department_name,
        CASE 
            WHEN CAST("código_del_municipio_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_municipio_ies" AS INTEGER)
            ELSE -1 
        END                                                                      AS ies_municipality_id,
        CASE 
            WHEN TRIM("municipio_de_domicilio_de_la_ies") = '' OR "municipio_de_domicilio_de_la_ies" IS NULL THEN 'Unknown'
            ELSE TRIM("municipio_de_domicilio_de_la_ies")
        END                                                                      AS ies_municipality_name,

        -- ── Academic Programme ────────────────────────────────────────────────
        CASE 
            WHEN CAST("código_snies_del_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_snies_del_programa" AS INTEGER)
            ELSE -1 
        END                                                                      AS program_id,
        CASE 
            WHEN TRIM("programa_académico") = '' OR "programa_académico" IS NULL THEN 'Unknown'
            ELSE TRIM("programa_académico")
        END                                                                      AS program_name,
        TRIM("programa_acreditado")                                                 AS is_program_accredited,

        -- ── Academic Level ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_nivel_académico" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_nivel_académico" AS INTEGER)
            ELSE -1 
        END                                                                      AS academic_level_id,
        CASE 
            WHEN TRIM("nivel_académico") = '' OR "nivel_académico" IS NULL THEN 'Unknown'
            ELSE TRIM("nivel_académico")
        END                                                                      AS academic_level_name,

        -- ── Formation Level ───────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_nivel_de_formación" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_nivel_de_formación" AS INTEGER)
            ELSE -1 
        END                                                                      AS formation_level_id,
        CASE 
            WHEN TRIM("nivel_de_formación") = '' OR "nivel_de_formación" IS NULL THEN 'Unknown'
            ELSE TRIM("nivel_de_formación")
        END                                                                      AS formation_level_name,

        -- ── Modality (Methodology) ───────────────────────────────────────────
        CASE 
            WHEN CAST("id_modalidad" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_modalidad" AS INTEGER)
            ELSE -1 
        END                                                                      AS methodology_id,
        TRIM("modalidad")                                                           AS methodology_name,

        -- ── Knowledge Area ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_área" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_área" AS INTEGER)
            ELSE -1 
        END                                                                      AS knowledge_area_id,
        TRIM("área_de_conocimiento")                                                AS knowledge_area_name,

        -- ── NBC (Núcleo Básico del Conocimiento) ──────────────────────────────
        CASE 
            WHEN CAST("id_núcleo" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_núcleo" AS INTEGER)
            ELSE -1 
        END                                                                      AS nbc_id,
        TRIM("núcleo_básico_del_conocimiento_nbc")                                  AS nbc_name,

        -- ── CINE Classification ───────────────────────────────────────────────
        CASE 
            WHEN CAST("id_cine_campo_amplio" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_amplio" AS INTEGER)
            ELSE -1 
        END                                                                      AS cine_broad_field_id,
        CASE 
            WHEN TRIM("desc_cine_campo_amplio") = '' OR "desc_cine_campo_amplio" IS NULL THEN 'Unknown'
            ELSE TRIM("desc_cine_campo_amplio")
        END                                                                      AS cine_broad_field_name,
        CASE 
            WHEN CAST("id_cine_campo_especifico" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_especifico" AS INTEGER)
            ELSE -1 
        END                                                                      AS cine_specific_field_id,
        CASE 
            WHEN TRIM("desc_cine_campo_especifico") = '' OR "desc_cine_campo_especifico" IS NULL THEN 'Unknown'
            ELSE TRIM("desc_cine_campo_especifico")
        END                                                                      AS cine_specific_field_name,
        CASE 
            WHEN CAST("id_cine_campo_detallado" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_detallado" AS INTEGER)
            ELSE -1 
        END                                                                      AS cine_detailed_field_id,
        CASE 
            WHEN TRIM("desc_cine_campo_detallado") = '' OR "desc_cine_campo_detallado" IS NULL THEN 'Unknown'
            ELSE TRIM("desc_cine_campo_detallado")
        END                                                                      AS cine_detailed_field_name,

        -- ── Programme Offer Geography ─────────────────────────────────────────
        CASE 
            WHEN CAST("código_del_departamento_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_departamento_programa" AS INTEGER)
            ELSE -1 
        END                                                                      AS program_department_id,
        CASE 
            WHEN TRIM("departamento_de_oferta_del_programa") = '' OR "departamento_de_oferta_del_programa" IS NULL THEN 'Unknown'
            ELSE TRIM("departamento_de_oferta_del_programa")
        END                                                                      AS program_department_name,
        CASE 
            WHEN CAST("código_del_municipio_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_municipio_programa" AS INTEGER)
            ELSE -1 
        END                                                                      AS program_municipality_id,
        CASE 
            WHEN TRIM("municipio_de_oferta_del_programa") = '' OR "municipio_de_oferta_del_programa" IS NULL THEN 'Unknown'
            ELSE TRIM("municipio_de_oferta_del_programa")
        END                                                                      AS program_municipality_name,

        -- ── Gender ────────────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_sexo" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_sexo" AS INTEGER)
            ELSE -1 
        END                                                                      AS gender_id,
        CASE 
            WHEN TRIM("sexo") = '' OR "sexo" IS NULL THEN 'Unknown'
            ELSE TRIM("sexo")
        END                                                                      AS gender_name,

        -- ── Time Dimension & Metric ───────────────────────────────────────────
        CASE 
            WHEN CAST("año" AS TEXT) ~ '^[0-9]+$' THEN CAST("año" AS INTEGER)
            ELSE -1 
        END                                                                      AS data_year,
        CASE 
            WHEN CAST("semestre" AS TEXT) ~ '^[0-9]+$' THEN CAST("semestre" AS INTEGER)
            ELSE -1 
        END                                                                      AS semester,
        CASE 
            WHEN CAST("matriculados" AS TEXT) ~ '^[0-9]+$' THEN CAST("matriculados" AS INTEGER)
            ELSE 0 
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
            ELSE -1 
        END                                                                      AS institution_id,
        CASE 
            WHEN CAST("ies_padre" AS TEXT) ~ '^[0-9]+$' THEN CAST("ies_padre" AS INTEGER)
            ELSE NULL 
        END                                                                      AS parent_institution_id,
        CASE 
            WHEN TRIM("institución_de_educación_superior_ies") = '' OR "institución_de_educación_superior_ies" IS NULL THEN 'Unknown'
            ELSE TRIM("institución_de_educación_superior_ies")
        END                                                                      AS institution_name,
        TRIM("tipo_ies")                                                            AS institution_type,
        TRIM("ies_acreditada")                                                       AS is_institution_accredited,

        -- ── Sector ───────────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_sector_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_sector_ies" AS INTEGER)
            ELSE -1 
        END                                                                      AS sector_id,
        TRIM("sector_ies")                                                          AS sector_name,

        -- ── Character (Carácter) ──────────────────────────────────────────────
        CASE 
            WHEN CAST("id_carácter_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_carácter_ies" AS INTEGER)
            ELSE -1 
        END                                                                      AS character_id,
        CASE 
            WHEN TRIM("carácter_ies") = '' OR "carácter_ies" IS NULL THEN 'Unknown'
            ELSE TRIM("carácter_ies")
        END                                                                      AS character_name,

        -- ── IES Geography ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("código_del_departamento_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_departamento_ies" AS INTEGER)
            ELSE -1 
        END                                                                      AS ies_department_id,
        CASE 
            WHEN TRIM("departamento_de_domicilio_de_la_ies") = '' OR "departamento_de_domicilio_de_la_ies" IS NULL THEN 'Unknown'
            ELSE TRIM("departamento_de_domicilio_de_la_ies")
        END                                                                      AS ies_department_name,
        CASE 
            WHEN CAST("código_del_municipio_ies" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_municipio_ies" AS INTEGER)
            ELSE -1 
        END                                                                      AS ies_municipality_id,
        CASE 
            WHEN TRIM("municipio_de_domicilio_de_la_ies") = '' OR "municipio_de_domicilio_de_la_ies" IS NULL THEN 'Unknown'
            ELSE TRIM("municipio_de_domicilio_de_la_ies")
        END                                                                      AS ies_municipality_name,

        -- ── Academic Programme ────────────────────────────────────────────────
        CASE 
            WHEN CAST("código_snies_del_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_snies_del_programa" AS INTEGER)
            ELSE -1 
        END                                                                      AS program_id,
        CASE 
            WHEN TRIM("programa_académico") = '' OR "programa_académico" IS NULL THEN 'Unknown'
            ELSE TRIM("programa_académico")
        END                                                                      AS program_name,
        TRIM("programa_acreditado")                                                 AS is_program_accredited,

        -- ── Academic Level ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_nivel_académico" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_nivel_académico" AS INTEGER)
            ELSE -1 
        END                                                                      AS academic_level_id,
        CASE 
            WHEN TRIM("nivel_académico") = '' OR "nivel_académico" IS NULL THEN 'Unknown'
            ELSE TRIM("nivel_académico")
        END                                                                      AS academic_level_name,

        -- ── Formation Level ───────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_nivel_de_formación" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_nivel_de_formación" AS INTEGER)
            ELSE -1 
        END                                                                      AS formation_level_id,
        CASE 
            WHEN TRIM("nivel_de_formación") = '' OR "nivel_de_formación" IS NULL THEN 'Unknown'
            ELSE TRIM("nivel_de_formación")
        END                                                                      AS formation_level_name,

        -- ── Modality (Methodology) ───────────────────────────────────────────
        CASE 
            WHEN CAST("id_modalidad" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_modalidad" AS INTEGER)
            ELSE -1 
        END                                                                      AS methodology_id,
        TRIM("modalidad")                                                           AS methodology_name,

        -- ── Knowledge Area ────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_área" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_área" AS INTEGER)
            ELSE -1 
        END                                                                      AS knowledge_area_id,
        TRIM("área_de_conocimiento")                                                AS knowledge_area_name,

        -- ── NBC (Núcleo Básico del Conocimiento) ──────────────────────────────
        CASE 
            WHEN CAST("id_núcleo" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_núcleo" AS INTEGER)
            ELSE -1 
        END                                                                      AS nbc_id,
        TRIM("núcleo_básico_del_conocimiento_nbc")                                  AS nbc_name,

        -- ── CINE Classification ───────────────────────────────────────────────
        CASE 
            WHEN CAST("id_cine_campo_amplio" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_amplio" AS INTEGER)
            ELSE -1 
        END                                                                      AS cine_broad_field_id,
        CASE 
            WHEN TRIM("desc_cine_campo_amplio") = '' OR "desc_cine_campo_amplio" IS NULL THEN 'Unknown'
            ELSE TRIM("desc_cine_campo_amplio")
        END                                                                      AS cine_broad_field_name,
        CASE 
            WHEN CAST("id_cine_campo_especifico" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_especifico" AS INTEGER)
            ELSE -1 
        END                                                                      AS cine_specific_field_id,
        CASE 
            WHEN TRIM("desc_cine_campo_especifico") = '' OR "desc_cine_campo_especifico" IS NULL THEN 'Unknown'
            ELSE TRIM("desc_cine_campo_especifico")
        END                                                                      AS cine_specific_field_name,
        CASE 
            WHEN CAST("id_cine_campo_detallado" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_cine_campo_detallado" AS INTEGER)
            ELSE -1 
        END                                                                      AS cine_detailed_field_id,
        CASE 
            WHEN TRIM("desc_cine_campo_detallado") = '' OR "desc_cine_campo_detallado" IS NULL THEN 'Unknown'
            ELSE TRIM("desc_cine_campo_detallado")
        END                                                                      AS cine_detailed_field_name,

        -- ── Programme Offer Geography ─────────────────────────────────────────
        CASE 
            WHEN CAST("código_del_departamento_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_departamento_programa" AS INTEGER)
            ELSE -1 
        END                                                                      AS program_department_id,
        CASE 
            WHEN TRIM("departamento_de_oferta_del_programa") = '' OR "departamento_de_oferta_del_programa" IS NULL THEN 'Unknown'
            ELSE TRIM("departamento_de_oferta_del_programa")
        END                                                                      AS program_department_name,
        CASE 
            WHEN CAST("código_del_municipio_programa" AS TEXT) ~ '^[0-9]+$' THEN CAST("código_del_municipio_programa" AS INTEGER)
            ELSE -1 
        END                                                                      AS program_municipality_id,
        CASE 
            WHEN TRIM("municipio_de_oferta_del_programa") = '' OR "municipio_de_oferta_del_programa" IS NULL THEN 'Unknown'
            ELSE TRIM("municipio_de_oferta_del_programa")
        END                                                                      AS program_municipality_name,

        -- ── Gender ────────────────────────────────────────────────────────────
        CASE 
            WHEN CAST("id_sexo" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_sexo" AS INTEGER)
            ELSE -1 
        END                                                                      AS gender_id,
        CASE 
            WHEN TRIM("sexo") = '' OR "sexo" IS NULL THEN 'Unknown'
            ELSE TRIM("sexo")
        END                                                                      AS gender_name,

        -- ── Time Dimension & Metric ───────────────────────────────────────────
        CASE 
            WHEN CAST("año" AS TEXT) ~ '^[0-9]+$' THEN CAST("año" AS INTEGER)
            ELSE -1 
        END                                                                      AS data_year,
        CASE 
            WHEN CAST("semestre" AS TEXT) ~ '^[0-9]+$' THEN CAST("semestre" AS INTEGER)
            ELSE -1 
        END                                                                      AS semester,
        CASE 
            WHEN CAST("matriculados" AS TEXT) ~ '^[0-9]+$' THEN CAST("matriculados" AS INTEGER)
            ELSE 0
        END                                                                      AS enrollment_count,

        -- ── Audit ─────────────────────────────────────────────────────────────
        "loaded_at"                                                                 AS loaded_at

    FROM source_2024
)

SELECT * FROM renamed