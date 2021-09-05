
/*
    Tables
*/

WITH art_blocks AS (

    SELECT 
        *
    FROM 
        {{ ref('stg_art_blocks_sales') }}

),

crypto_punks AS (

    SELECT 
        *
    FROM 
        {{ ref('inter_crypto_punks_transactions_unioned') }}

),

/*
    Transformations
*/

sales__unioned AS (

    SELECT 
        buyer_account_id, 
        seller_account_id, 
        token_id, 
        payment_token_id,
        block_at,
        block_number,
        project_name,
        artist_name, 
        transaction_type,
        price
    FROM 
        art_blocks

    UNION ALL 
    
    SELECT 
        buyer_account_id, 
        seller_account_id, 
        token_id, 
        payment_token_id,
        block_at,
        block_number,
        project_name,
        artist_name, 
        transaction_type,
        price
    FROM 
        crypto_punks

)

SELECT * FROM sales__unioned
