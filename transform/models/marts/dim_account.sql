/*
    Tables
*/

WITH accounts AS (

    SELECT 
        *
    FROM 
        {{ ref('inter_platform_all_accounts') }}

),

/*
    Cleaning
*/

formatted AS (

    SELECT 
        -- PK
        ROW_NUMBER() OVER(ORDER BY account_id DESC) AS dim_account_id, 

        -- Details
        account_id
    FROM 
        accounts

)

SELECT * FROM formatted
