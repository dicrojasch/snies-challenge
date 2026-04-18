-- Silver schema view for Estudiantes Inscritos 2022
-- Standardizing columns and aggregating to Institution level

WITH raw_estudiantes AS (
    SELECT *
    FROM {{ source('bronze', 'estudiantes_inscritos_2022') }}
),
cleaned AS (
    SELECT 
        CAST(codigo_ies AS INT) AS ies_id,
        TRIM(nombre_ies) AS ies_name,
        CAST(ano AS INT) AS data_year,
        CAST(inscritos AS INT) AS total_enrolled
    FROM raw_estudiantes
    -- Filtering for Bogota if there's a geography column. Assuming 'municipio_ies' exists.
    -- WHERE TRIM(LOWER(municipio_ies)) = 'bogota' OR TRIM(LOWER(municipio_ies)) = 'bogotá'
)
SELECT 
    ies_id,
    ies_name,
    data_year,
    SUM(total_enrolled) as total_enrolled_students
FROM cleaned
GROUP BY 1, 2, 3
