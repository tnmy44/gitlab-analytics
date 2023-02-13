WITH source AS (

    SELECT
    {{ hash_sensitive_columns('sfdc_task_source') }}
    FROM {{ ref('sfdc_task_source') }}

)

SELECT 
  *,
  full_comments_hash     AS full_comments,
  task_subject_hash      AS task_subject
FROM source

