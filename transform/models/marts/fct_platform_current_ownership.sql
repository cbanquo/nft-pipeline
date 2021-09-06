/*
    Tables
*/

WITH 
-- accounts AS (

--     SELECT 
--         *
--     FROM 
--         {{ ref('dim_account') }}

-- ),

-- projects AS (

--     SELECT 
--         *
--     FROM 
--         {{ ref('dim_project') }}

-- ),

inter_token_current_owner AS (

    SELECT 
        *
    FROM 
        {{ ref('inter_platform_token_current_owner') }}

), 

/*
    Transformations
*/

-- current_owner_dims__joined AS (

--     SELECT 
--         accounts.dim_account_id, 
--         projects.dim_project_id,
--         inter_token_current_owner.*
--     FROM 
--         inter_token_current_owner
--     INNER JOIN 
--         accounts
--     ON 
--        accounts.account_id = inter_token_current_owner.buyer_account_id
--     INNER JOIN 
--         projects
--     USING 
--         (project_name, artist_name)

-- ),

/*
    Cleaning 
*/

formatted AS (

    SELECT 
        -- FK 
        buyer_account_id AS account_id, 
        contract_id, 
        token_id,

        -- Details
        block_at AS bought_at,
        eth_price
        
    FROM 
        inter_token_current_owner

)

SELECT * FROM formatted
