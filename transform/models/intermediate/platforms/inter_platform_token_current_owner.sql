/*
    Who currently owns each project/token?
*/

/*
    Tables
*/

WITH transactions AS (

    SELECT 
        * 
    FROM 
        {{ ref('inter_platform_historical_transactions') }}

), 

/*
    Transformations
*/

transactions__last AS (

    SELECT
        * 
    FROM 
        transactions__row_number
    WHERE 
        desc_transaction_number = 1

)

SELECT * FROM transactions__last
