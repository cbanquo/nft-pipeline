/*
    Flatten buyer and seller account ids to a long list
*/


/*
    Tables
*/

WITH transactions AS (

    SELECT 
        buyer_account_id, 
        seller_account_id
    FROM 
        {{ ref('inter_project_transactions__unioned') }}

),

/*
    Transformations
*/

accounts__unioned AS (

    SELECT 
        buyer_account_id AS account_id
    FROM 
        transactions

    UNION ALL 

    SELECT 
        seller_account_id
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
