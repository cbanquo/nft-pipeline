/*
    Tables
*/

WITH opensea_transactions AS (

    SELECT 
        *
    FROM 
        {{ source('raw_python', 'art_blocks_sales')}}

),

/*
    Transformations
*/

-- wouldn't normally tansform but needed to flatten the JSON
flattened AS (

    SELECT 
        --FK 
        data:buyer::TEXT AS buyer_account_id,
        data:seller::TEXT AS seller_account_id, 
        'ART-BLOCKS-' || value:token:tokenId::TEXT AS token_id,
        data:paymentToken::TEXT AS payment_token_id,

        -- Timestamps
        TO_TIMESTAMP(data:blockTimestamp) AS block_at, 
        data:blockNumber::INT AS block_number, 

        
        -- Details
        'Sale' AS transfer_type
        value:token:project:name::TEXT AS project_name,
        value:token:project:artistName::TEXT AS artist_name,
        data:saleType::TEXT AS sale_type,

        -- Numbers
        data:price::INT AS price
        
    FROM 
        opensea_transactions,
        LATERAL FLATTEN(input => data:tokenOpenSeaSaleLookupTables) 

)

SELECT * FROM flattened
