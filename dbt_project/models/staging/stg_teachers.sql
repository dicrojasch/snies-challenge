-- =============================================================================
-- Staging: stg_teachers
-- Purpose : Cast, rename, and lightly clean the raw Bronze teachers table.
--           No business logic or aggregation — one row in, one row out.
-- =============================================================================

WITH source_2022 AS (
    SELECT * FROM {{ source('bronze', 'docentes_2022') }}
),

source_2023 AS (
    SELECT * FROM {{ source('bronze', 'docentes_2023') }}
),

source_2024 AS (
    SELECT * FROM {{ source('bronze', 'docentes_2024') }}
),

renamed AS (
    -- =============================================================================
    -- Staging: stg_teachers_2022
    -- Purpose : Cast, rename, and lightly clean the raw Bronze teachers table (2022).
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

            -- ── Teacher Profile ──────────────────────────────────────────────────
            CASE 
                WHEN CAST("id_sexo" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_sexo" AS INTEGER)
                ELSE -1 
            END                                                                      AS gender_id,
            CASE 
                WHEN TRIM("sexo_del_docente") = '' OR "sexo_del_docente" IS NULL THEN 'Unknown'
                ELSE TRIM("sexo_del_docente")
            END                                                                      AS gender_name,
            
            CASE 
                WHEN CAST("id_máximo_nivel_de_formación_del_docente" AS TEXT) ~ '^[0-9]+$' 
                THEN CAST("id_máximo_nivel_de_formación_del_docente" AS INTEGER)
                ELSE -1 
            END                                                                      AS formation_level_id,
            CASE 
                WHEN TRIM("máximo_nivel_de_formación_del_docente") = '' OR "máximo_nivel_de_formación_del_docente" IS NULL THEN 'Unknown'
                ELSE TRIM("máximo_nivel_de_formación_del_docente")
            END                                                                      AS formation_level_name,
            -- TRIM("nivel_cine")                                                          AS cine_level,

            -- ── Work Conditions ──────────────────────────────────────────────────
            CASE 
                WHEN CAST("id_tiempo_de_dedicación" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_tiempo_de_dedicación" AS INTEGER)
                ELSE -1 
            END                                                                      AS dedication_time_id,
            CASE 
                WHEN TRIM("tiempo_de_dedicación_del_docente") = '' OR "tiempo_de_dedicación_del_docente" IS NULL THEN 'Unknown'
                ELSE TRIM("tiempo_de_dedicación_del_docente")
            END                                                                      AS dedication_time_name,
            
            CASE 
                WHEN CAST("id_tipo_de_contrato" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_tipo_de_contrato" AS INTEGER)
                ELSE -1 
            END                                                                      AS contract_type_id,
            CASE 
                WHEN TRIM("tipo_de_contrato_del_docente") = '' OR "tipo_de_contrato_del_docente" IS NULL THEN 'Unknown'
                ELSE TRIM("tipo_de_contrato_del_docente")
            END                                                                      AS contract_type_name,

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
                WHEN CAST("no_de_docentes" AS TEXT) ~ '^[0-9]+$' THEN CAST("no_de_docentes" AS INTEGER)
                ELSE 0
            END                                                                      AS teacher_count,

            -- ── Audit ─────────────────────────────────────────────────────────────
            "loaded_at"                                                                 AS loaded_at

        FROM source_2022

        UNION ALL 
        -- =============================================================================
        -- Staging: stg_teachers_2023
        -- Purpose : Cast, rename, and lightly clean the raw Bronze teachers table (2023).
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
            -- TRIM("ies_acreditada")                                                       AS is_institution_accredited,

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

            -- ── Teacher Profile ──────────────────────────────────────────────────
            CASE 
                WHEN CAST("id_sexo" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_sexo" AS INTEGER)
                ELSE -1 
            END                                                                      AS gender_id,
            CASE 
                WHEN TRIM("sexo_del_docente") = '' OR "sexo_del_docente" IS NULL THEN 'Unknown'
                ELSE TRIM("sexo_del_docente")
            END                                                                      AS gender_name,
            
            CASE 
                WHEN CAST("id_máximo_nivel_de_formación_del_docente" AS TEXT) ~ '^[0-9]+$' 
                THEN CAST("id_máximo_nivel_de_formación_del_docente" AS INTEGER)
                ELSE -1 
            END                                                                      AS formation_level_id,
            CASE 
                WHEN TRIM("máximo_nivel_de_formación_del_docente") = '' OR "máximo_nivel_de_formación_del_docente" IS NULL THEN 'Unknown'
                ELSE TRIM("máximo_nivel_de_formación_del_docente")
            END                                                                      AS formation_level_name,

            -- ── Work Conditions ──────────────────────────────────────────────────
            CASE 
                WHEN CAST("id_tiempo_de_dedicación" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_tiempo_de_dedicación" AS INTEGER)
                ELSE -1 
            END                                                                      AS dedication_time_id,
            CASE 
                WHEN TRIM("tiempo_de_dedicación_del_docente") = '' OR "tiempo_de_dedicación_del_docente" IS NULL THEN 'Unknown'
                ELSE TRIM("tiempo_de_dedicación_del_docente")
            END                                                                      AS dedication_time_name,
            
            CASE 
                WHEN CAST("id_tipo_de_contrato" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_tipo_de_contrato" AS INTEGER)
                ELSE -1 
            END                                                                      AS contract_type_id,
            CASE 
                WHEN TRIM("tipo_de_contrato") = '' OR "tipo_de_contrato" IS NULL THEN 'Unknown'
                ELSE TRIM("tipo_de_contrato")
            END                                                                      AS contract_type_name,

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
                WHEN CAST("docentes" AS TEXT) ~ '^[0-9]+$' THEN CAST("docentes" AS INTEGER)
                ELSE 0
            END                                                                      AS teacher_count,

            -- ── Audit ─────────────────────────────────────────────────────────────
            "loaded_at"                                                                 AS loaded_at

        FROM source_2023

        UNION ALL

    -- =============================================================================
    -- Staging: stg_teachers_2024
    -- Purpose : Cast, rename, and lightly clean the raw Bronze teachers table (2024).
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
            -- TRIM("ies_acreditada")                                                       AS is_institution_accredited,

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

            -- ── Teacher Profile ──────────────────────────────────────────────────
            CASE 
                WHEN CAST("id_sexo" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_sexo" AS INTEGER)
                ELSE -1 
            END                                                                      AS gender_id,
            CASE 
                WHEN TRIM("sexo_del_docente") = '' OR "sexo_del_docente" IS NULL THEN 'Unknown'
                ELSE TRIM("sexo_del_docente")
            END                                                                      AS gender_name,
            
            CASE 
                WHEN CAST("id_máximo_nivel_de_formación_del_docente" AS TEXT) ~ '^[0-9]+$' 
                THEN CAST("id_máximo_nivel_de_formación_del_docente" AS INTEGER)
                ELSE -1 
            END                                                                      AS formation_level_id,
            CASE 
                WHEN TRIM("máximo_nivel_de_formación_del_docente") = '' OR "máximo_nivel_de_formación_del_docente" IS NULL THEN 'Unknown'
                ELSE TRIM("máximo_nivel_de_formación_del_docente")
            END                                                                      AS formation_level_name,

            -- ── Work Conditions ──────────────────────────────────────────────────
            CASE 
                WHEN CAST("id_tiempo_de_dedicación" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_tiempo_de_dedicación" AS INTEGER)
                ELSE -1 
            END                                                                      AS dedication_time_id,
            CASE 
                WHEN TRIM("tiempo_de_dedicación_del_docente") = '' OR "tiempo_de_dedicación_del_docente" IS NULL THEN 'Unknown'
                ELSE TRIM("tiempo_de_dedicación_del_docente")
            END                                                                      AS dedication_time_name,
            
            CASE 
                WHEN CAST("id_tipo_de_contrato" AS TEXT) ~ '^[0-9]+$' THEN CAST("id_tipo_de_contrato" AS INTEGER)
                ELSE -1 
            END                                                                      AS contract_type_id,
            CASE 
                WHEN TRIM("tipo_de_contrato") = '' OR "tipo_de_contrato" IS NULL THEN 'Unknown'
                ELSE TRIM("tipo_de_contrato")
            END                                                                      AS contract_type_name,

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
                WHEN CAST("docentes" AS TEXT) ~ '^[0-9]+$' THEN CAST("docentes" AS INTEGER)
                ELSE 0
            END                                                                      AS teacher_count,

            -- ── Audit ─────────────────────────────────────────────────────────────
            "loaded_at"                                                                 AS loaded_at

        FROM source_2024
)

SELECT * FROM renamed
