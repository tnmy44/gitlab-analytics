WITH source AS (
  SELECT * FROM
    {{ source('adaptive', 'attributes') }}
)

SELECT
  PARSE_JSON(_data) ['@id']::VARCHAR           AS parent_id,
  PARSE_JSON(_data) ['@name']::VARCHAR         AS parent_name,
  PARSE_JSON(_data) ['@type']::VARCHAR         AS parent_type,
  PARSE_JSON(_data) ['@dimension-id']::VARCHAR AS parent_dimension_id,
  PARSE_JSON(_data) ['@seqNo']::VARCHAR        AS parent_seq_num,
  attribute_values.value['@id']::VARCHAR       AS id,
  attribute_values.value['@name']::VARCHAR     AS name,
  __loaded_at
FROM
  source,
  LATERAL FLATTEN(input => PARSE_JSON(_data) ['attributeValue']) attribute_values
