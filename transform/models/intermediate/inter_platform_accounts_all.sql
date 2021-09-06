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

accounts__grouped AS (

    SELECT 
        account_id
    FROM 
        accounts__unioned
    GROUP BY 
        1

)

SELECT * FROM accounts__grouped
