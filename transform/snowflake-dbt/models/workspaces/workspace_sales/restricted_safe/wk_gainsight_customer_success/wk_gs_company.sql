WITH source AS (

    SELECT *
    FROM {{ ref('company') }}

)

SELECT *
FROM source