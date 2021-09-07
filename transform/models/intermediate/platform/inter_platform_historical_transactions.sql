/*
    Long list of all transactions that happened on a platform
*/


/*
    Tables
*/

WITH open_sea_sales AS (

    SELECT 
        * 
    FROM 
        {{ ref('stg_platform_open_sea_sales') }}

),

/*
    Transformations
*/

-- union all platforms transactions
transactions__unioned AS (

    SELECT
        *
    FROM    
        open_sea_sales

),

-- get price in ETH
transaction_price__joined AS (

    SELECT
        *,
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
        ROW_NUMBER() OVER(PARTITION BY contract_id, token_id ORDER BY block_at DESC) AS desc_transaction_number -- 1 is most recent transaction
    FROM 
        transaction_price__joined
)

SELECT * FROM transaction_number
