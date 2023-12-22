WITH source AS (

    SELECT *
    FROM {{ ref('zuora_query_api_charge_contractual_value_source') }}
)
SELECT *
FROM source
