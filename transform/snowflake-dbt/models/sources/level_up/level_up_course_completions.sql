
with source as
(select * FROM {{ source('level_up', 'course_completions') }} ),

  intermediate AS (
    SELECT
      d.value,
      source.uploaded_at
    FROM
      source,
      LATERAL FLATTEN(input => source.jsontext['data']) AS d
  ),

parsed as (
select
  value['companyId']::varchar as company_id,
  value['event']::varchar as event,
  value['id']::varchar as id,
  value['license']::varchar as license,
  value['notifiableId']::varchar as notifiableId,
  value['source']::varchar as source,
  value['timestamp']::varchar as timestamp,
  value['title']::varchar as title,
  value['type']::varchar as transaction_type,
  value['updatedAt']::varchar as updated_at,
  value['userDetail']['client']::varchar as  client,
  value['userDetail']['id']::varchar as  user_id,
  value['userDetail']['ref1']::varchar as  user_type,
  value['userDetail']['ref2']::varchar as  user_job,
  value['userDetail']['ref3']::varchar as  ref3,
  value['userDetail']['ref4']::varchar as  user_company,
  value['userDetail']['ref6']::varchar as  ref6,
  value['userDetail']['ref7']::varchar as  user_continent,
  value['userDetail']['ref8']::varchar as  user_country,
  value['userDetail']['ref9']::varchar as  user_sub_dept,
  value['userDetail']['ref10']::varchar as  user_dept,
  uploaded_at
from intermediate

-- remove dups in case 'raw' is reloaded
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      id
    ORDER BY
      uploaded_at DESC
  ) = 1
)
select * from parsed

