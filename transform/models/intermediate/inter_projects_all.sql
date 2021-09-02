/*
    Tables
*/

WITH transactions AS (

    SELECT 
        project_name, 
        artist_name
    FROM 
        {{ ref('stg_art_blocks_sales') }}

),

/*
    Transformations
*/

projects__grouped AS (

    SELECT 
        project_name, 
        artist_name
    FROM 
        transactions
    GROUP BY 
        1, 2

)

SELECT * FROM projects__grouped
