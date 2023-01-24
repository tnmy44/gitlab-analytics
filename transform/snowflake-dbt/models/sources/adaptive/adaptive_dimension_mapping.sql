WITH source AS (
  SELECT * FROM
    {{ source('adaptive', 'dimension_mapping') }}
),

parsed AS (
  SELECT
    PARSE_JSON(_data) v,
    __loaded_at
  FROM
    source
)

SELECT
  v:version['@id']::varchar                                                                   AS parent_id,
  v:version['@name']::varchar                                                                 AS parent_name,
  v:version['@readOnly']::varchar                                                             AS parent_readonly,
  v:dimensions.dimension['@id']::varchar                                                      AS dimension_id,
  v:dimensions.dimension['@mappingDimensionIds']::varchar                                     AS dimension_mappingdimensionids,
  v:dimensions.dimension['@name']::varchar                                                    AS dimension_name,
  v:dimensions.dimension.mappingCriteria.mappingCriterion['@id']::varchar                     AS dimension_mapping_criteria_id,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.dimension['@id']::varchar           AS dimension_mapping_criteria_dimension_id,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.dimension['@type']::varchar         AS dimension_mapping_criteria_dimension_type,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.dimension['@valueId']::varchar      AS dimension_mapping_criteria_dimension_value_id,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.mapTo['@dimensionValueId']::varchar AS map_to_dimension_value_id,
  __loaded_at
FROM
  parsed
ORDER BY __loaded_at
