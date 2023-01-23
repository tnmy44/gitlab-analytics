
WITH source AS (

    SELECT *
    FROM {{ source('static', 'qualtrics_post_purchase_survey_answers_from_issue_15225') }}

)
SELECT *
FROM source
