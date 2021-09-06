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
        value:token:tokenId::TEXT AS token_id,
        data:paymentToken::TEXT AS payment_token_id,

        -- Timestamps
        TO_TIMESTAMP(data:blockTimestamp) AS block_at, 
        data:blockNumber::INT AS block_number, 

        -- Details
        'Sale' AS transaction_type,
        value:token:project:name::TEXT AS project_name,
        data:saleType::TEXT AS sale_type,

        -- Numbers
        array_size(data:tokenOpenSeaSaleLookupTables) AS transaction_size,
        data:price / transaction_size AS price
        
    FROM 
        opensea_transactions,
        LATERAL FLATTEN(input => data:tokenOpenSeaSaleLookupTables) 
    WHERE
        LOWER(project_name) IN ('fidenza', 
                                'ringers', 
                                'chromie squiggle', 
                                'archetype', 
                                'elevated deconstructions') 

)

SELECT * FROM flattened
