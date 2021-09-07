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

current_owners AS (

    SELECT 
        *
    FROM 
        {{ ref('inter_platform_historical_transactions') }}
    WHERE
        desc_transaction_number = 1

), 

/*
    Transformations
*/

current_owner_dims__joined AS (

    SELECT 
        _to.dim_account_id AS to_dim_account_id, 
        _from.dim_account_id AS from_dim_account_id, 
        projects.dim_project_id,
        current_owners.*
    FROM 
        current_owners
    INNER JOIN 
        accounts AS _to
    ON 
       _to.account_id = current_owners.to_account_id
    INNER JOIN 
        accounts AS _from
    ON 
       _from.account_id = current_owners.from_account_id
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
