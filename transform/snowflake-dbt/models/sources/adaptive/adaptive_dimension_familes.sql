WITH
  parsed AS (
    SELECT
      PARSE_JSON(_data) v,
      __LOADED_AT
    FROM
      "RAW"."TAP_ADAPTIVE"."DIMENSION_FAMILIES"
  )
select
v['@accounts']::varchar AS accounts,
v['@dimensions']::varchar AS dimensions,
v['@name']::varchar AS name,
v['@id']::varchar as id
from parsed
