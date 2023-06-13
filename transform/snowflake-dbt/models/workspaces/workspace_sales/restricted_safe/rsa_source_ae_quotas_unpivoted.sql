{{
    config(
        materialized='view'
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('ae_quotas_unpivoted') }}

)
SELECT *
FROM source