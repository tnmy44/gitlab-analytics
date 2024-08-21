{{ config(
    materialized='incremental',
    unique_key='id'
) }}

{{ level_up_incremental('email_captures') }}

parsed AS (
  SELECT
  value['notifiableType']::varchar as notifiable_type,
  value['courseSku']::varchar as course_sku,
  value['courseId']::varchar as course_id,
  value['companyId']::varchar as company_id,
  value['companyHost']::varchar as company_host,
  value['companySubdomain']::varchar as company_subdomain,
  value['courseTitle']::varchar as course_title,
  value['anonymousId']::varchar as anonymous_id,
  value['source']::varchar as source,
  value['notifiableId']::varchar as notifiable_id,
  value['timestamp']::timestamp as event_timestamp,
  {{ level_up_filter_gitlab_email("value['user']") }} as username,
  value['event']::varchar as event,
  SHA2(concat(course_id, value['user'], event_timestamp)) as id,

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
