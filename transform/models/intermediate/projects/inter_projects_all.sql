/*
    Tables
*/

WITH sales AS (

    SELECT 
        project_name, 
        artist_name
    FROM 
        {{ ref('inter_transactions_unioned') }}

),

/*
    Transformations
*/

projects__grouped AS (

    SELECT 
        project_name, 
        artist_name
    FROM 
        sales
    GROUP BY 
        1, 2

)

SELECT * FROM projects__grouped
