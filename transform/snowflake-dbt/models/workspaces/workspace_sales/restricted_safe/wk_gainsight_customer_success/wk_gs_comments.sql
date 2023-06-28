WITH source AS (

    SELECT *
    FROM {{ ref('comments') }}

)

SELECT *
FROM source