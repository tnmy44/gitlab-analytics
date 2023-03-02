with source as
(select * FROM {{ source('level_up', 'logins') }} ),

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
      value['browserInfo']::variant as  browser_info,
      value['companyHost']::varchar as companyHost,
      value['companyId']::varchar as companyId,
      value['companySubdomain']::varchar as companySubdomain,
      value['event']::varchar as event,
      value['ipAddress']::varchar as ipAddress,
      value['ipGeoInfo']::variant as ipGeoInfo,
      value['timestamp']::varchar as timestamp,
      value['userAgent']::varchar as userAgent,
      value['userDetail']['id']::varchar as user_id,
      value['userDetail']['ref1']::varchar as user_type,
      value['userDetail']['ref2']::varchar as user_job,
      value['userDetail']['ref3']::varchar as ref3,
      value['userDetail']['sfAccountId']::varchar as sfAccountId,
      value['userDetail']['sfContactId']::varchar as sfContactId,
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

