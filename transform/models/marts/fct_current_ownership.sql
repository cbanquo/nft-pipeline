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
        {{ ref('inter_platform_token_current_owner') }}

), 

/*
    Transformations
*/

current_owner_dims__joined AS (

    SELECT 
        _to.dim_account_id AS to_dim_account_id, 
        _from.dim_account_id AS from_dim_account_id, 
        projects.dim_project_id,
        inter_token_current_owner.*
    FROM 
        inter_token_current_owner
    INNER JOIN 
        accounts AS _to
    ON 
       _to.account_id = inter_token_current_owner.to_account_id
    INNER JOIN 
        accounts AS _from
    ON 
       _from.account_id = inter_token_current_owner.from_account_id
    INNER JOIN 
        projects
    USING 
        (contract_id)

),

/*
    Cleaning 
*/

formatted AS (

    SELECT 
        -- FK 
        to_dim_account_id,
        from_dim_account_id,
        dim_project_id, 

        -- Details
        token_id,
        block_at AS bought_at,
        eth_price
        
    FROM 
        current_owner_dims__joined

)

SELECT * FROM formatted
