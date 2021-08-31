/*
    Tables
*/

WITH accounts AS (

    SELECT 
        *
    FROM 
        {{ ref('dim_account') }}

),

inter_token_current_owner AS (

    SELECT 
        *
    FROM 
        {{ ref('inter_token_current_owner') }}

), 

/*
    Transformations
*/

accounts_current_owner__joined AS (

    SELECT 
        accounts.dim_account_id, 
        inter_token_current_owner.*
    FROM 
        accounts
    INNER JOIN 
        inter_token_current_owner
    ON 
       accounts.account_id = inter_token_current_owner.buyer_account_id

),

/*
    Cleaning 
*/

formatted AS (

    SELECT 
        -- FK 
        dim_account_id, 
        project_id, 
        token_id
    FROM 
        accounts_current_owner__joined

)

SELECT * FROM formatted
