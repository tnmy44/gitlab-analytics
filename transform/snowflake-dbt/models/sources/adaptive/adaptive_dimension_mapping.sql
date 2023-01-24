
with source as (
  select * from
  {{ source('tap_adaptive', 'dimension_mapping'}}
),
  parsed AS (
    SELECT
      PARSE_JSON(_data) v,
      __LOADED_AT
    FROM
      source
  )
SELECT
  v:version['@id']::varchar AS parent_id,
  v:version['@name']::varchar AS parent_name,
  v:version['@readOnly']::varchar AS parent_readOnly,
  v:dimensions.dimension['@id']::varchar AS dimension_id,
  v:dimensions.dimension['@mappingDimensionIds']::varchar AS dimension_mappingDimensionIds,
  v:dimensions.dimension['@name']::varchar AS dimension_name,
  v:dimensions.dimension.mappingCriteria.mappingCriterion['@id']::varchar AS dimension_mapping_criteria_id,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.dimension['@id']::varchar AS dimension_mapping_criteria_dimension_id,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.dimension['@type']::varchar AS dimension_mapping_criteria_dimension_type,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.dimension['@valueId']::varchar AS dimension_mapping_criteria_dimension_value_id,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.mapTo['@dimensionValueId']::varchar AS map_to_dimension_value_id,
  __LOADED_AT
FROM
  parsed
order by __LOADED_AT
