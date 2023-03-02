with source as
(select * FROM {{ source('level_up', 'visits') }} ),

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
value['companyHost']::varchar as company_host,
value['companyId']::varchar as company_id,
value['companySubdomain']::varchar as company_subdomain,
value['event']::varchar as event,
value['timestamp']::varchar as timestamp,
    value['userDetail']['clientId']::varchar as client_id,
    value['userDetail']['departmentId']::varchar as department_id,
    value['userDetail']['id']::varchar as user_id,
    value['userDetail']['ref1']::varchar as user_type,
    value['userDetail']['ref2']::varchar as user_job,
    value['userDetail']['ref3']::varchar as ref3,
    value['userDetail']['sfAccountId']::varchar as sf_account_id,
    value['userDetail']['sfContactId']::varchar as sf_contact_id,
  uploaded_at
from intermediate

-- remove dups in case 'raw' is reloaded
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      user_id,
      timestamp
    ORDER BY
      uploaded_at DESC
  ) = 1
)
select * from parsed

