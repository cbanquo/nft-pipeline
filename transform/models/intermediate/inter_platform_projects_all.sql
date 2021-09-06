/*
    Tables
*/

WITH projects AS (

    SELECT 
        contract_id
    FROM 
        {{ ref('inter_platform_transactions__unioned') }}

),

/*
    Transformations
*/

projects__grouped AS (

    SELECT 
        contract_id
    FROM 
        projects
    GROUP BY 
        1

)

SELECT * FROM projects__grouped
