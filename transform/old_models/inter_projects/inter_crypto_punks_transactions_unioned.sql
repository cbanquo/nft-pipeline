
/*
    Tables
*/

WITH sales AS (

    SELECT 
        * 
    FROM 
        {{ ref('stg_crypto_punk_sales') }}

), 

transfers AS (

    SELECT 
        *
    FROM 
        {{ ref('stg_crypto_punk_transfers') }}

),

/*
    Transformations
*/

unioned AS (

    SELECT 
        buyer_account_id,
        seller_account_id,
        token_id,
        payment_token_id,
        block_at,
        block_number,
        transaction_type,
        project_name,
        artist_name,
        price
    FROM 
        sales

    UNION ALL 

    SELECT
        buyer_account_id,
        seller_account_id,
        token_id,
        payment_token_id,
        block_at,
        block_number,
        transaction_type,
        project_name,
        artist_name,
        price
    FROM 
        transfers

)

SELECT * FROM unioned
