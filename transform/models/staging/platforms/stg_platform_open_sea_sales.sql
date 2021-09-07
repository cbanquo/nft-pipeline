/*
    Tables
*/

WITH open_sea_sales AS (

    SELECT 
        *
    FROM 
        {{ source('raw_python', 'open_sea_sales')}}

),

/*
    Transformations
*/

-- wouldn't normally tansform but needed to flatten the JSON
flattened AS (

    SELECT 
        --FK 
        data:buyer::TEXT AS to_account_id,
        data:seller::TEXT AS from_account_id, 
        value:nft:tokenId::INT AS token_id,
        value:nft:contract:id::TEXT AS contract_id,
        data:paymentToken::TEXT AS payment_token_id,

        -- Timestamps
        TO_TIMESTAMP(data:blockTimestamp) AS block_at, 
        data:blockNumber::INT AS block_number, 

        -- Details
        'Sale' AS transaction_type,
        data:saleType::TEXT AS sale_type,

        -- Numbers
        array_size(data:nftOpenSeaSaleLookupTable) AS transaction_size,
        data:price / transaction_size AS price
        
    FROM 
        open_sea_sales,
        LATERAL FLATTEN(input => data:nftOpenSeaSaleLookupTable) 

)

SELECT * FROM flattened
