/*
    Get all accounts that have interacted with any platform
*/

/*
    Tables
*/

WITH transactions AS (

    SELECT 
        to_account_id, 
        from_account_id
    FROM 
        {{ ref('inter_platform_transactions__unioned') }}

),

/*
    Transformations
*/

-- long single list
accounts__unioned AS (

    SELECT 
        to_account_id AS account_id
    FROM 
        transactions

    UNION ALL 

    SELECT 
        from_account_id
    FROM 
        transactions

), 

-- remove duplicates
accounts__distinct AS (

    SELECT 
        DISTINCT account_id
    FROM 
        accounts__unioned

)

SELECT * FROM accounts__distinct
