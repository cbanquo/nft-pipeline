WITH crypto_punk_sales AS (

    SELECT 
        *
    FROM 
        {{ source('raw_python', 'crypto_punk_transfers')}}

),

/*
    Cleaning
*/

formatted AS (

    SELECT 
        -- FK 
        data:to:id::TEXT AS buyer_account_id, 
        data:from:id::TEXT AS seller_account_id, 
        'CRYPTO-PUNK-' || data:nft:tokenId::TEXT AS token_id,
        '0x0000000000000000000000000000000000000000'::TEXT AS payment_token_id,

        -- Timestamps
        TO_TIMESTAMP(data:timestamp) AS block_at,
        data:block::INT AS block_number,

        -- Details
        'Transfer' AS transfer_type,
        'Crypto Punks' AS project_name, 
        'Larva Labs' AS artist_name,

        -- Numbers
        0 AS price
        
    FROM 
        crypto_punk_sales

)

SELECT * FROM formatted
