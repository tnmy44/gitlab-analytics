WITH source AS (

    SELECT *
    FROM {{ ref('call_to_action') }}

)

SELECT *
FROM source