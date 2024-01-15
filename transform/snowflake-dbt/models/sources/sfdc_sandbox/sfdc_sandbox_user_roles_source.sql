WITH base AS (

    SELECT * 
    FROM {{ source('salesforce_sandbox', 'user_role') }}

)

SELECT *
FROM base