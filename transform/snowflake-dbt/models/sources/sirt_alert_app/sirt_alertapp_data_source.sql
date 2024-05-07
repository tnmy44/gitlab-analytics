WITH
source AS (
  SELECT
  alertid::int as alertid,
  ack_time::int as ack_time,
  alert_name::varchar as alert_name,
  alert_time::int as alert_time,
  assignee::varchar as assignee,
  change_assignee::varchar as change_assignee,
  close_alert::int as close_alert,
  comments::varchar as comments,
  convert_incident::boolean as convert_incident,
  to_date(db_date, 'YY-MM-DD') db_date,
  event_time::int as event_time,
  feature::varchar as feature,
  label::varchar as label,
  mitre_tac::varchar as mitre_tac,
  mitre_teq::varchar as mitre_teq,
  sentiment::varchar as sentiment,
  gcs_uploaded_at::date as gcs_uploaded_at,
  snowflake_uploaded_at::datetime as snowflake_uploaded_at
  FROM
    {{ source('sirt_alertapp', 'sirt_alertapp_data') }}
),

dedupped AS (
  SELECT *
  FROM source
  QUALIFY ROW_NUMBER() OVER (PARTITION BY alertid ORDER BY gcs_uploaded_at DESC) = 1
)

SELECT *
FROM dedupped
