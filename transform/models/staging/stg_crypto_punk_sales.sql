WITH crypto_punk_sales AS (

    SELECT 
        *
    FROM 
        {{ source('raw_python', 'crypto_punk_sales')}}

),

/*
    Cleaning
*/

formatted AS (

    SELECT 
        -- FK 
        data:to:id::TEXT AS buyer_account_id, 
        data:from:id::TEXT AS seller_account_id, 

        'CRYPTO-PUNK-' || SPLIT_PART(data:id ,'-', 1)::TEXT AS token_id,

        '0x0000000000000000000000000000000000000000'::TEXT AS payment_token_id,

        -- Timestamps
        TO_TIMESTAMP(data:timestamp) AS block_at,

        -- Details
        data:blockNumber::INT AS block_number,

        data:contract:name::TEXT AS project_name, 
        'Larva Labs' AS artist_name,

        'Single' AS sale_type,
        data:amount::INT AS price
        
    FROM 
        crypto_punk_sales

)

SELECT * FROM formatted
