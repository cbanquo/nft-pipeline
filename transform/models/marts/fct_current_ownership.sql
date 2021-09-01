/*
    Tables
*/

WITH accounts AS (

    SELECT 
        *
    FROM 
        {{ ref('dim_account') }}

),

projects AS (

    SELECT 
        *
    FROM 
        {{ ref('dim_project') }}

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

current_owner_dims__joined AS (

    SELECT 
        accounts.dim_account_id, 
        projects.dim_project_id,
        inter_token_current_owner.*
    FROM 
        inter_token_current_owner
    INNER JOIN 
        accounts
    ON 
       accounts.account_id = inter_token_current_owner.buyer_account_id
    INNER JOIN 
        projects
    USING 
        (project_id)

),

/*
    Cleaning 
*/

formatted AS (

    SELECT 
        -- FK 
        dim_account_id, 
        dim_project_id, 
        token_id,

        -- Details
        block_at AS bought_at
        
    FROM 
        current_owner_dims__joined

)

SELECT * FROM formatted
