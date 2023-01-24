
with source as (
  select * from
  {{ source('tap_adaptive', 'dimension_families'}}
),
  parsed AS (
    SELECT
      PARSE_JSON(_data) v,
      __LOADED_AT
    FROM
      source
  )
select
v['@accounts']::varchar AS accounts,
v['@dimensions']::varchar AS dimensions,
v['@name']::varchar AS name,
v['@id']::varchar as id
from parsed
