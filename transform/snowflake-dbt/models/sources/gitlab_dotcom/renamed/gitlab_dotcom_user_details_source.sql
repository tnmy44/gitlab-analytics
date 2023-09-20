WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_user_details_dedupe_source') }}

), renamed AS (

    SELECT
      user_id::NUMBER                     AS user_id,
      job_title::VARCHAR                  AS job_title,
      registration_objective::NUMBER      AS registration_objective,
      organization::VARCHAR               AS user_organization,
      TRY_TO_NUMBER(discord)              AS user_discord
    FROM source
    
)

SELECT  *
FROM renamed
