/*
    Who currently owns each project/token?
*/

/*
    Tables
*/

WITH sales AS (

    SELECT 
        * 
    FROM 
        {{ ref('inter_sales_unioned') }}

), 

/*
    Transformations
*/

sales__row_number AS (

    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY project_name, token_id ORDER BY block_number DESC) AS rn
    FROM 
        sales
), 

sales__last AS (

    SELECT
        * 
    FROM 
        sales__row_number
    WHERE 
        rn = 1

)

SELECT * FROM sales__last
