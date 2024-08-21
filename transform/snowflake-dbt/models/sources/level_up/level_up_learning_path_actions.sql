{{ config(
    materialized='incremental',
    unique_key='id'
) }}

{{ level_up_incremental('learning_path_actions') }}

parsed AS (
  SELECT
  value['id']::varchar as id,
  value['source']::varchar as source,
  value['timestamp']::timestamp as event_timestamp,
  value['type']::varchar as type,
  {{ level_up_filter_gitlab_email("value['user']") }} as username,
  value['companyId']::varchar as company_id,
  value['notifiableId']::varchar as notifiable_id,
  value['learningPathTitle']::varchar as learning_path_title,
  value['learningPathSku']::varchar as learning_path_sku,
  value['milestoneTitle']::varchar as milestone_title,
  value['userDetail']['id']::varchar as user_id,
  value['userDetail']['sfContactId']::varchar as sf_contact_id,
  value['userDetail']['sfAccountId']::varchar as sf_account_id,

  uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
