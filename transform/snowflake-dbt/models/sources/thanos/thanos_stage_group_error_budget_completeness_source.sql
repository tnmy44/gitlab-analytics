WITH source AS (
  SELECT *
  FROM {{ source('thanos', 'periodic_queries') }}

),

parsed AS (

  SELECT
    PARSE_JSON(parse_json_2.value:value[0]::DOUBLE)::INT::TIMESTAMP_NTZ AS metric_created_at,
    NULLIF(parse_json_2.value:value[1], 'NaN')::DOUBLE                  AS metric_value,
    parse_json_1.value['data']['resultType']::VARCHAR                   AS result_type,
    parse_json_1.value['status']::VARCHAR                               AS status_type,
    parse_json_1.this['message']::VARCHAR                               AS message_type,
    parse_json_1.this['status_code']::VARCHAR                           AS status_code,
    parse_json_1.this['success']::VARCHAR                               AS is_success
  FROM
    source,
    LATERAL FLATTEN(input => source.jsontext['stage_group_error_budget_completeness']) parse_json_1,
    LATERAL FLATTEN(input => source.jsontext['stage_group_error_budget_completeness']['body']['data']['result'], outer => TRUE) parse_json_2
  WHERE result_type IS NOT NULL AND status_type IS NOT NULL

)

SELECT * FROM
  parsed
