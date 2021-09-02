/*
    Flatten buyer and seller account ids to a long list
*/


/*
    Tables
*/

WITH sales AS (

    SELECT 
        buyer_account_id, 
        seller_account_id
    FROM 
        {{ ref('inter_sales_unioned') }}

),

/*
    Transformations
*/

accounts__unioned AS (

    SELECT 
        buyer_account_id AS account_id
    FROM 
        sales

    UNION ALL 

    SELECT 
        seller_account_id
    FROM 
        sales

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
