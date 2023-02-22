WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_reconciliations') }}

)

SELECT *
FROM source
