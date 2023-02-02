WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_notification_settings_dedupe_source') }}
  
), renamed AS (

    SELECT

      id::NUMBER                                      AS notification_settings_id,
      user_id::NUMBER                                 AS user_id,
      source_id::NUMBER                               AS source_id,
      created_at::TIMESTAMP                           AS created_at,
      updated_at::TIMESTAMP                           AS updated_at,

    FROM source

)

SELECT *
FROM renamed
