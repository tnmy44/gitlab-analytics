
with source as (
  select * from
  {{ source('tap_adaptive', 'attributes'}}
),
SELECT
  PARSE_JSON(_data) ['@id']::varchar AS parent_id,
  PARSE_JSON(_data) ['@name']::varchar AS parent_name,
  PARSE_JSON(_data) ['@type']::varchar AS parent_type,
  PARSE_JSON(_data) ['@dimension-id']::varchar AS parent_dimension_id,
  PARSE_JSON(_data) ['@seqNo']::varchar AS parent_seq_num,
  attribute_values.value['@id']::varchar AS id,
  attribute_values.value['@name']::varchar AS name
FROM
  source,
  LATERAL FLATTEN(input => PARSE_JSON(_data)['attributeValue']) attribute_values
