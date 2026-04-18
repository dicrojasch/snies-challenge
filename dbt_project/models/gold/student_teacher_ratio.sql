-- Gold schema table for BI Tools and Analytics
-- Calculates the Student-to-Teacher Ratio and joins with the SUE classification seed.

WITH students AS (
    SELECT * FROM {{ ref('stg_estudiantes') }}
),
teachers AS (
    SELECT * FROM {{ ref('stg_docentes') }}
),
sue_mapping AS (
    SELECT * FROM {{ ref('sue_institutions') }}
),
joined_data AS (
    SELECT 
        COALESCE(s.ies_id, t.ies_id) AS ies_id,
        COALESCE(s.ies_name, t.ies_name) AS ies_name,
        COALESCE(s.data_year, t.data_year) AS data_year,
        COALESCE(s.total_enrolled_students, 0) AS total_enrolled_students,
        COALESCE(t.total_teachers, 0) AS total_teachers,
        
        -- Business Rule: Student-to-Teacher Ratio
        CASE 
            WHEN COALESCE(t.total_teachers, 0) > 0 THEN 
                CAST(COALESCE(s.total_enrolled_students, 0) AS FLOAT) / t.total_teachers
            ELSE NULL 
        END AS student_to_teacher_ratio
        
    FROM students s
    FULL OUTER JOIN teachers t 
      ON s.ies_id = t.ies_id AND s.data_year = t.data_year
)
SELECT 
    j.*,
    -- Business Rule: SUE mapping indicator
    COALESCE(sm.is_sue, FALSE) AS is_sue_institution
FROM joined_data j
LEFT JOIN sue_mapping sm ON j.ies_id = sm.ies_id
