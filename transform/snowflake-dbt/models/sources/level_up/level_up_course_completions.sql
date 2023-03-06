WITH
source AS (
  SELECT * FROM
    {{ source('level_up', 'course_completions') }}
),

intermediate AS (
  SELECT
    d.value,
    source.uploaded_at
  FROM
    source,
    LATERAL FLATTEN(input => source.jsontext['data']) AS d
),

parsed AS (
  SELECT
    value['id']::VARCHAR                   AS id,
    value['companyId']::VARCHAR            AS company_id,
    value['event']::VARCHAR                AS event,
    value['license']::VARCHAR              AS license,
    value['notifiableId']::VARCHAR         AS notifiable_id,
    value['source']::VARCHAR               AS course_action,
    value['timestamp']::TIMESTAMP          AS event_timestamp,
    value['title']::VARCHAR                AS title,
    value['type']::VARCHAR                 AS transaction_type,
    value['updatedAt']::TIMESTAMP          AS updated_at,
    value['userDetail']['id']::VARCHAR     AS user_id,
    value['userDetail']['client']::VARCHAR AS client,
    value['userDetail']['ref1']::VARCHAR   AS ref1_user_type,
    value['userDetail']['ref2']::VARCHAR   AS ref2_user_job,
    value['userDetail']['ref3']::VARCHAR   AS ref3,
    value['userDetail']['ref4']::VARCHAR   AS ref4_user_company,
    value['userDetail']['ref6']::VARCHAR   AS ref6,
    value['userDetail']['ref7']::VARCHAR   AS ref7_user_continent,
    value['userDetail']['ref8']::VARCHAR   AS ref8_user_country,
    value['userDetail']['ref9']::VARCHAR   AS ref9_user_sub_dept,
    value['userDetail']['ref10']::VARCHAR  AS ref10_user_dept,
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
