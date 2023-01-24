WITH
  parsed AS (
    SELECT
      PARSE_JSON(_data) v,
      __LOADED_AT
    FROM
      "RAW"."TAP_ADAPTIVE"."DIMENSION_FAMILIES"
  )
select
v['@accounts']::varchar,
v['@dimensions']::varchar,
v['@name']::varchar,
v['@id']::varchar
from parsed;
