WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_editor_ai_conversation_messages_source') }}

)

SELECT *
FROM source
