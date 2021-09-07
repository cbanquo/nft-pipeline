/*
    Tables
*/

WITH projects AS (

    SELECT 
        *
    FROM 
        {{ ref('inter_platform_projects_all') }}

),

/*
    Cleaning
*/

formatted AS (

    SELECT
        -- PK
        ROW_NUMBER() OVER(ORDER BY contract_id) AS dim_project_id,

        -- Timestamps
        first_seen_at,

        -- Details
        contract_id

    FROM 
        projects

)

SELECT * FROM formatted
