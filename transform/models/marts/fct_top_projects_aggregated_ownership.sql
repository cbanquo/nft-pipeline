-- projects to aggregate for
{% set top_projects = ["fidenza", "ringers", "crypto_punks"] %}


/*
    Tables
*/

WITH last_transaction AS (

    SELECT 
        *
    FROM 
        {{ ref('inter_top_projects_historical_transactions') }}
    WHERE
        desc_transaction_number = 1

), 

accounts AS (

    SELECT
        *
    FROM 
        {{ ref('dim_account') }}

),

/*
    Transformations
*/

-- only include accounts from dim_account (as these are accounts that have interacted with our platforms of interest)
accounts_transaction__joined AS (

    SELECT
        accounts.dim_account_id,
        last_transaction.*
    FROM 
        last_transaction
    INNER JOIN 
        accounts
    ON 
        last_transaction.to_account_id = accounts.account_id

),

-- aggregate ownership of top projects for each account
agg_ownership AS (

    SELECT
        dim_account_id,
        {% for top_project in top_projects %}
            COUNT(DISTINCT CASE WHEN LOWER(project_name) = '{{top_project}}' THEN token_id END) AS n_{{top_project}}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    FROM 
        accounts_transaction__joined
    GROUP BY 
        1

)

SELECT * FROM agg_ownership
