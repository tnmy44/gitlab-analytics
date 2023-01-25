WITH source AS (
  SELECT * FROM
    {{ source('adaptive', 'dimensions') }}
)

SELECT
  PARSE_JSON(_data) ['@id']::VARCHAR              AS parent_id,
  PARSE_JSON(_data) ['@name']::VARCHAR            AS parent_name,
  PARSE_JSON(_data) ['@seqNo']::VARCHAR           AS parent_seq_num,
  dimension_values.value['@id']::VARCHAR          AS id,
  dimension_values.value['@name']::VARCHAR        AS name,
  dimension_values.value['@description']::VARCHAR AS description,
  dimension_values.value['@shortName']::VARCHAR   AS short_name,
  dimension_values.value['attributes']::VARIANT   AS attributes,
  __loaded_at
FROM
  source,
  LATERAL FLATTEN(input => PARSE_JSON(_data) ['dimensionValue']) dimension_values
