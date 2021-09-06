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
        {{ ref('inter_project_transactions__unioned') }}

), 

/*
    Transformations
*/

transactions__row_number AS (

    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY project_name, token_id ORDER BY block_number DESC) AS rn
    FROM 
        transactions
), 

transactions__last AS (

    SELECT
        * 
    FROM 
        transactions__row_number
    WHERE 
        rn = 1

)

SELECT * FROM transactions__last
