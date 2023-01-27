WITH source AS (
  SELECT * FROM {{ source('adaptive', 'attributes') }}
)

SELECT
  PARSE_JSON(source._data) ['@id']::VARCHAR           AS parent_id,
  PARSE_JSON(source._data) ['@name']::VARCHAR         AS parent_name,
  PARSE_JSON(source._data) ['@type']::VARCHAR         AS parent_type,
  PARSE_JSON(source._data) ['@dimension-id']::VARCHAR AS parent_dimension_id,
  PARSE_JSON(source._data) ['@seqNo']::VARCHAR        AS parent_seq_num,
  attribute_values.value['@id']::VARCHAR              AS id,
  attribute_values.value['@name']::VARCHAR            AS attribute_name,
  source.__loaded_at                                  AS uploaded_at
FROM
  source,
  LATERAL FLATTEN(input => PARSE_JSON(source._data) ['attributeValue']) AS attribute_values
