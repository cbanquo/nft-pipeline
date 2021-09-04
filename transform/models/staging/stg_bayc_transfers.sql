
/*
    Tables
*/

WITH transfers AS (

    SELECT 
        *
    FROM 
        {{ source('raw_python', 'bayc_transfers') }}

),

/*
    Cleaing
*/

formatted AS (

    SELECT
        -- FK
        data:to:id::TEXT AS buyer_account_id, 
        data:from:id::TEXT AS seller_account_id,
        'BAYC-' || data:token:id::TEXT AS token_id,
        NULL AS payment_token_id,

        -- Timestamps
        data:block::INT AS block_number,
        TO_TIMESTAMP(data:timestamp) AS block_at, 
        
        -- Details
        'Transfer' AS transfer_type
        'Bored Ape Yacht Club'::TEXT AS project_name,
        NULL AS artist_name,
        
        -- Numbers
        0 AS price

    FROM
        transfers

)

SELECT * FROM formatted
