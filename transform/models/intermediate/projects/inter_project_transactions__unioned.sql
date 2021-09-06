
/*
    Tables
*/

WITH art_blocks AS (

    SELECT 
        *
    FROM 
        {{ ref('stg_project_art_blocks_sales') }}

),

crypto_punks AS (

    SELECT 
        *
    FROM 
        {{ ref('inter_project_crypto_punks_transactions__unioned') }}

),

/*
    Transformations
*/

transactions__unioned AS (

    SELECT 
        buyer_account_id, 
        seller_account_id, 
        token_id, 
        payment_token_id,
        block_at,
        block_number,
        project_name,
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
        transaction_type,
        price
    FROM 
        crypto_punks

),

-- convert price to ETH
price_normalised AS (

    SELECT 
        buyer_account_id, 
        seller_account_id, 
        token_id, 
        block_at,
        block_number,
        project_name,
        transaction_type,
        ((price / rate) / 1000000000000000000)::FLOAT AS eth_price
    FROM 
        transactions__unioned
    INNER JOIN 
        {{ ref('currency_conversion') }}
    USING
        (payment_token_id)

)

SELECT * FROM price_normalised
