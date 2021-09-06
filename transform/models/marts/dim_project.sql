/*
    Tables
*/

WITH projects AS (

    SELECT 
        *
    FROM 
        {{ ref('inter_platform_projects_all') }}

),

-- projects we have identified as elite
top_projects AS (

    SELECT
        *
    FROM 
        {{ ref('top_projects') }}

),

/*
    Transformations
*/

-- flag top projects
projects_top__joined AS (

    SELECT
        projects.*,
        top_projects.contract_id IS NOT NULL AS is_top_project
    FROM
        projects
    LEFT JOIN 
        top_projects
    USING 
        (contract_id)

),

/*
    Cleaning
*/

formatted AS (

    SELECT
        -- PK
        ROW_NUMBER() OVER(ORDER BY contract_id) AS dim_project_id,

        -- Details
        contract_id,

        -- Bools 
        is_top_project

    FROM 
        projects_top__joined

)

SELECT * FROM formatted
