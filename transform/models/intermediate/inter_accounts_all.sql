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
        {{ ref('stg_art_blocks_sales') }}

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
        buyer_account_id
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
