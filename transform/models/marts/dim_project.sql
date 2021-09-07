/*
    Tables
*/

WITH contracts AS (

    SELECT 
        *
    FROM 
        {{ ref('inter_platform_all_contracts') }}

),

/*
    Cleaning
*/

formatted AS (

    SELECT
        -- PK
        ROW_NUMBER() OVER(ORDER BY contract_id) AS dim_project_id,

        -- Timestamps
        first_transaction_at,

        -- Details
        contract_id

    FROM 
        contracts

)

SELECT * FROM formatted
