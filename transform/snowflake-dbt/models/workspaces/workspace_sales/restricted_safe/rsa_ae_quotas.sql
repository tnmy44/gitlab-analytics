{{
    config(
        materialized='view'
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('rsa_ae_quotas_source') }}

)
SELECT *
FROM source