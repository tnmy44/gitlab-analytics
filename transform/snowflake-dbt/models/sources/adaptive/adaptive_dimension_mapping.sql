WITH
  parsed AS (
    SELECT
      PARSE_JSON(_data) v,
      __LOADED_AT
    FROM
      "RAW"."TAP_ADAPTIVE"."DIMENSION_MAPPING"
  )
SELECT
  v:version['@id']::varchar,
  v:version['@name']::varchar,
  v:version['@readOnly']::varchar,
  v:dimensions.dimension['@id']::varchar,
  v:dimensions.dimension['@mappingDimensionIds']::varchar,
  v:dimensions.dimension['@name']::varchar,
  v:dimensions.dimension.mappingCriteria.mappingCriterion['@id']::varchar,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.dimension['@id']::varchar,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.dimension['@type']::varchar,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.dimension['@valueId']::varchar,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.mapTo['@dimensionValueId']::varchar,
  __LOADED_AT
FROM
  parsed
order by __LOADED_AT
