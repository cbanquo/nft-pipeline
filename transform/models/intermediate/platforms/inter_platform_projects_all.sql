/*
    Tables
*/

WITH projects AS (

    SELECT 
        contract_id, 
        block_at
    FROM 
        {{ ref('inter_platform_transactions__unioned') }}

),

/*
    Transformations
*/

projects__grouped AS (

    SELECT 
        contract_id,
        MIN(block_at) AS first_seen_at
    FROM 
        projects
    GROUP BY 
        1

)

SELECT * FROM projects__grouped
