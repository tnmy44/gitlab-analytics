WITH source AS (

    SELECT *
    FROM {{ ref('zuora_query_api_charge_metrics_source') }}
)
SELECT *
FROM source
