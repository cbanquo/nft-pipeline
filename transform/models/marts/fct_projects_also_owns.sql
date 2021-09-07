/*
    Tables
*/

WITH platform_ownership AS (

    SELECT
        * 
    FROM 
        {{ ref('inter_platform_historical_transactions') }}
    WHERE   
        desc_transaction_number

), 

project_ownership AS (

    SELECT 
        *
    FROM 
        {{ ref('inter_project_token_current_owner') }}

),

/*
    Transformations
*/

platform_project_ownership__joined AS (

    SELECT 
        platform_ownership.*,
        project_ownership.project_name AS top_project_name,
        project_ownership.token_id AS top_token_id  
    FROM 
        platform_ownership
    LEFT JOIN 
        project_ownership
    USING 
        (to_account_id)

),

platform_project__grouped AS (

    SELECT 
        contract_id, 
        COUNT(DISTINCT CASE WHEN top_project_name IS NOT NULL THEN to_account_id END) AS n_top_owners

    FROM 
        platform_project_ownership__joined
    GROUP BY 
        1

),

/*
    Cleaning
*/

formatted AS (

    SELECT
        * 
    FROM 
        platform_project__grouped

)

SELECT * FROM formatted
