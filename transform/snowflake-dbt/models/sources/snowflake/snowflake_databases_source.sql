WITH source AS (

    SELECT *
    FROM {{ source('snowflake_account_usage','databases') }}

), intermediate AS (

    SELECT *
    FROM source

)

SELECT *
FROM intermediate