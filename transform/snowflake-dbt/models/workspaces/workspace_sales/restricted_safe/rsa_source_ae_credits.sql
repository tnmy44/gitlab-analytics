{{
    config(
        materialized='view'
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('ae_credits') }}

)
SELECT *
FROM source