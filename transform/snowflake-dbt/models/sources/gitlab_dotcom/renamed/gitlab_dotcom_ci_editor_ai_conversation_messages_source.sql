WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_editor_ai_conversation_messages_dedupe_source') }}
  
), renamed AS (

  SELECT 
    id::NUMBER              AS ci_editor_ai_conversation_messages_id,
    user_id::NUMBER         AS ci_editor_ai_conversation_messages_user_id, 
    project_id::NUMBER      AS ci_editor_ai_conversation_messages_project_id, 
    created_at::TIMESTAMP   AS created_at, 
    updated_at::TIMESTAMP   AS updated_at, 
    role::VARCHAR           AS role
  FROM source

)


SELECT *
FROM renamed
