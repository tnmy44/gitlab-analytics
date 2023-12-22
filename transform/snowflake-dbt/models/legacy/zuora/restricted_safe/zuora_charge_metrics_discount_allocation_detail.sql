WITH source AS (

    SELECT *
    FROM {{ ref('zuora_query_api_charge_metrics_discount_allocation_detail_source') }}
)
SELECT *
FROM source
