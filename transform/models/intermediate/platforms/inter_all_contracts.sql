/*
    Get all contracts that have interacted with any platform
*/

/*
    Tables
*/

WITH contracts AS (

    SELECT 
        contract_id, 
        block_at
    FROM 
        {{ ref('inter_platform_transactions__unioned') }}

),

/*
    Transformations
*/

-- 1 row per contract
contracts__grouped AS (

    SELECT 
        contract_id,
        MIN(block_at) AS first_transaction_at
    FROM 
        contracts
    GROUP BY 
        1

)

SELECT * FROM contracts__grouped
