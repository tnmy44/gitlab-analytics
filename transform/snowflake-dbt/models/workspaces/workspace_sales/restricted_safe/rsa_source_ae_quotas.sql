{{
    config(
        materialized='view'
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('ae_quotas') }}

)
SELECT *
FROM source