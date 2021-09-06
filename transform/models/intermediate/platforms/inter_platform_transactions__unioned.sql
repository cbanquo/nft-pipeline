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
platforms__unioned AS (

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
        platforms__unioned
    INNER JOIN 
        {{ ref('currency_conversion') }}
    USING
        (payment_token_id)

)

SELECT * FROM transaction_price__joined
