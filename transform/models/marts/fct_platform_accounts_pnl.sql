/*
    Tables
*/

WITH transactions AS (

    SELECT 
        *
    FROM 
        {{ ref('inter_platform_transactions__unioned') }}

),

-- accounts AS (

--     SELECT
--         * 
--     FROM 
--         {{ ref('dim_account') }}

-- ),

/*
    Transformations
*/

buys_sells__unioned AS (

    SELECT  
        buyer_account_id AS account_id, 
        'buy' AS type, 
        eth_price
    FROM 
        transactions

    UNION ALL 

    SELECT  
        seller_account_id, 
        'sell' AS type, 
        eth_price
    FROM 
        transactions

),

account_profit AS (

    SELECT 
        account_id,
        SUM(CASE WHEN type = 'buy' THEN 1 ELSE 0 END) AS n_buys, 
        COUNT(*) - n_buys AS n_sells, 
        SUM(
            CASE 
                WHEN type = 'sell'
                    THEN +eth_price
                ELSE 
                    -eth_price
            END
         ) AS eth_profit
    FROM 
        buys_sells__unioned
    GROUP BY 
        1

),

-- account_profit_dim__joined AS (

--     SELECT 
--         accounts.dim_account_id,
--         account_profit.*
--     FROM 
--         account_profit
--     INNER JOIN 
--         accounts
--     USING 
--         (account_id)

-- ),

/*
    Cleaning
*/

formmated AS (

    SELECT 
        --FK 
        account_id,

        --Details
        eth_profit, 
        n_buys, 
        n_sells
    
    FROM 
        account_profit
    ORDER BY 
        2 DESC

)

SELECT * FROM formmated
