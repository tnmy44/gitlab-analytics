WITH source AS (

    SELECT *
    FROM {{ source('snowflake_account_usage','tables') }}

), intermediate AS (

    SELECT *
    FROM source

)

SELECT *
FROM intermediate