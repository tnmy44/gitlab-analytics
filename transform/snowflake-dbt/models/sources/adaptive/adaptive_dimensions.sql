
with source as (
  select * from
  {{ source('tap_adaptive', 'dimensions'}}
),
SELECT
  PARSE_JSON(_data) ['@id']::varchar AS parent_id,
  PARSE_JSON(_data) ['@name']::varchar AS parent_name,
  PARSE_JSON(_data) ['@seqNo']::varchar AS parent_seq_num,
  dimension_values.value['@id']::varchar AS id,
  dimension_values.value['@name']::varchar AS name,
  dimension_values.value['@description']::varchar AS description,
  dimension_values.value['@shortName']::varchar AS short_name,
  dimension_values.value['@attribute']::variant AS attributes,
  __LOADED_AT
FROM
  source,
  LATERAL FLATTEN(input => PARSE_JSON(_data)['dimensionValue']) dimension_values
