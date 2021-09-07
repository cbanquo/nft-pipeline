/*
    Tables
*/

WITH accounts AS (

    SELECT 
        *
    FROM 
        {{ ref('inter_platform_all_accounts') }}

),

top_project_accounts AS (

    SELECT
        *
    FROM    
        {{ ref('inter_top_projects_all_accounts') }}

),

/*
    Transformations
*/

top_project_accounts__joined AS (

    SELECT
        accounts.*,
        top_project_accounts.account_id IS NOT NULL AS is_top_account
    FROM 
        accounts
    LEFT JOIN 
        top_project_accounts
    USING 
        (account_id)

),


/*
    Cleaning
*/

formatted AS (

    SELECT 
        -- PK
        ROW_NUMBER() OVER(ORDER BY account_id DESC) AS dim_account_id, 

        -- Details
        account_id,

        -- Bools
        is_top_account
    FROM 
        top_project_accounts__joined

)

SELECT * FROM formatted
