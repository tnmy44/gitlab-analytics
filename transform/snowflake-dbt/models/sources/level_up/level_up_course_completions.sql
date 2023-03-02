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
    value['companyId']::VARCHAR            AS company_id,
    value['event']::VARCHAR                AS event,
    value['id']::VARCHAR                   AS id,
    value['license']::VARCHAR              AS license,
    value['notifiableId']::VARCHAR         AS notifiable_id,
    value['source']::VARCHAR               AS course_action,
    value['timestamp']::TIMESTAMP          AS event_timestamp,
    value['title']::VARCHAR                AS title,
    value['type']::VARCHAR                 AS transaction_type,
    value['updatedAt']::TIMESTAMP          AS updated_at,
    value['userDetail']['client']::VARCHAR AS client,
    value['userDetail']['id']::VARCHAR     AS user_id,
    value['userDetail']['ref1']::VARCHAR   AS user_type,
    value['userDetail']['ref2']::VARCHAR   AS user_job,
    value['userDetail']['ref3']::VARCHAR   AS ref3,
    value['userDetail']['ref4']::VARCHAR   AS user_company,
    value['userDetail']['ref6']::VARCHAR   AS ref6,
    value['userDetail']['ref7']::VARCHAR   AS user_continent,
    value['userDetail']['ref8']::VARCHAR   AS user_country,
    value['userDetail']['ref9']::VARCHAR   AS user_sub_dept,
    value['userDetail']['ref10']::VARCHAR  AS user_dept,
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
