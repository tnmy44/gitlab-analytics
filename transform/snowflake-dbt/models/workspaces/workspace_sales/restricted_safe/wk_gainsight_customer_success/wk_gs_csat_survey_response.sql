WITH source AS (

    SELECT *
    FROM {{ ref('csat_survey_response') }}

)

SELECT *
FROM source