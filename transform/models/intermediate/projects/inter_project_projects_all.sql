/*
    Tables
*/

WITH sales AS (

    SELECT 
        project_name
    FROM 
        {{ ref('inter_project_transactions__unioned') }}

),

/*
    Transformations
*/

projects__grouped AS (

    SELECT 
        project_name
    FROM 
        sales
    GROUP BY 
        1

)

SELECT * FROM projects__grouped
