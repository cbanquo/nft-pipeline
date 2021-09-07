
/*
    Tables
*/

WITH sales AS (

    SELECT 
        * 
    FROM 
        {{ ref('stg_project_crypto_punk_sales') }}

), 

transfers AS (

    SELECT 
        *
    FROM 
        {{ ref('stg_project_crypto_punk_transfers') }}

),

/*
    Transformations
*/

unioned AS (

    SELECT 
        to_account_id,
        from_account_id,
        token_id,
        payment_token_id,
        block_at,
        block_number,
        transaction_type,
        project_name,
        price
    FROM 
        sales

    UNION ALL 

    SELECT
        to_account_id,
        from_account_id,
        token_id,
        payment_token_id,
        block_at,
        block_number,
        transaction_type,
        project_name,
        price
    FROM 
        transfers

)

SELECT * FROM unioned
