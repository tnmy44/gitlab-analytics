WITH source AS (

    SELECT *
    FROM {{ ref('cs_task') }}

)

SELECT *
FROM source