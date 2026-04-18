-- Silver schema view for Docentes 2022
-- Standardizing columns and aggregating to Institution level

WITH raw_docentes AS (
    SELECT *
    FROM {{ source('bronze', 'docentes_2022') }}
),
cleaned AS (
    SELECT 
        -- Assuming 'c_digo_de_la_instituci_n' is normalized to something similar
        -- We will alias them to standard names
        CAST(codigo_ies AS INT) AS ies_id,
        TRIM(nombre_ies) AS ies_name,
        CAST(ano AS INT) AS data_year,
        CAST(total_docentes AS INT) AS total_teachers
    FROM raw_docentes
    -- Filtering for Bogota if there's a geography column. Assuming 'municipio_ies' exists.
    -- WHERE TRIM(LOWER(municipio_ies)) = 'bogota' OR TRIM(LOWER(municipio_ies)) = 'bogotá'
)
SELECT 
    ies_id,
    ies_name,
    data_year,
    SUM(total_teachers) as total_teachers
FROM cleaned
GROUP BY 1, 2, 3
