
/*
    Tables
*/

WITH art_blocks AS (

    SELECT 
        *
    FROM 
        {{ ref('stg_top_project_art_blocks_sales') }}

),

crypto_punk_sales AS (

    SELECT 
        *
    FROM 
        {{ ref('stg_top_project_crypto_punk_sales') }}

),

crypto_punk_transfers AS (

    SELECT 
        *
    FROM 
        {{ ref('stg_top_project_crypto_punk_transfers') }}

),

/*
    Transformations
*/

transactions__unioned AS (

    SELECT 
        to_account_id, 
        from_account_id, 
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
        to_account_id, 
        from_account_id, 
        token_id, 
        payment_token_id,
        block_at,
        block_number,
        project_name,
        transaction_type,
        price
    FROM 
        crypto_punk_sales

    UNION ALL 
    
    SELECT 
        to_account_id, 
        from_account_id, 
        token_id, 
        payment_token_id,
        block_at,
        block_number,
        project_name,
        transaction_type,
        price
    FROM 
        crypto_punk_transfers

),

-- convert price to ETH
transaction_price__joined AS (

    SELECT 
        transactions__unioned.*,
        ((price / rate) / 1000000000000000000)::FLOAT AS eth_price
    FROM 
        transactions__unioned
    INNER JOIN 
        {{ ref('currency_conversion') }}
    USING
        (payment_token_id)

),

transaction_number AS (

    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY project_name, token_id ORDER BY block_at DESC) AS desc_transaction_number -- 1 is most recent transaction
    FROM 
        transaction_price__joined
)

SELECT * FROM transaction_number
