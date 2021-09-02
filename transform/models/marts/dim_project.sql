/*
    Tables
*/

WITH projects AS (

    SELECT 
        *
    FROM 
        {{ ref('inter_projects_all') }}

),

/*
    Cleaning
*/

formatted AS (

    SELECT
        -- PK
        ROW_NUMBER() OVER(ORDER BY project_name) AS dim_project_id,

        -- Details
        project_name, 
        artist_name

    FROM 
        projects

)

SELECT * FROM formatted
