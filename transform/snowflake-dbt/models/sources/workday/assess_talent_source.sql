WITH source AS (

    SELECT *
    FROM {{ source('workday','assess_talent') }}
  
), intermediate AS (

SELECT
    d.value AS data_by_row,
    source.employee_id,
    source._fivetran_synced, 
    source._fivetran_deleted
  FROM
    source,
    LATERAL FLATTEN(input => parse_json(KEY_TALENT_HISTORY_GROUP)) AS d
), parsed AS (


SELECT
  employee_id::NUMBER AS employee_id, 
  data_by_row['WORKDAY_ID']::VARCHAR              AS workday_id,
  data_by_row['KEY_TALENT']::VARCHAR              AS key_talent,
  data_by_row['EFFECTIVE_DATE']::VARCHAR          AS effective_date,
  _fivetran_synced::TIMESTAMP_TZ                  AS uploaded_at,
  _fivetran_deleted::BOOLEAN                      AS is_deleted
FROM
  intermediate

)

SELECT
* from parsed;