/*
    Tables
*/

WITH opensea_transactions AS (

    SELECT 
        *
    FROM 
        {{ source('raw_python', 'opensea_transactions')}}

),

/*
    Transformations
*/

-- wouldn't normally tansform but needed to flatten the JSON
flattened AS (

    SELECT 
        -- PK 
        data:id::TEXT AS transaction_id, 

        --FK 
        data:buyer::TEXT AS buyer_account_id,
        data:seller::TEXT AS seller_account_id, 
        
        value:token:project:projectId::INT AS project_id,
        value:token:tokenId::INT AS token_id,

        data:paymentToken::TEXT AS payment_token_id,

        -- Details
        data:blockNumber::INT AS block_number, 
        data:saleType::TEXT AS sale_type,
        data:price::INT AS price
        
    FROM 
        opensea_transactions,
        LATERAL FLATTEN(input => data:tokenOpenSeaSaleLookupTables) 

)

SELECT * FROM flattened
