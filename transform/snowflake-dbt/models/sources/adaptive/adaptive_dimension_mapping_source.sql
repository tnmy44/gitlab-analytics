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
  v:version['@id']::VARCHAR                                                                   AS version_id,
  v:version['@name']::VARCHAR                                                                 AS version_name,
  v:version['@readOnly']::VARCHAR                                                             AS version_readonly,
  v:dimensions.dimension['@id']::VARCHAR                                                      AS dimension_id,
  v:dimensions.dimension['@mappingDimensionIds']::VARCHAR                                     AS mapping_dimension_ids,
  v:dimensions.dimension['@name']::VARCHAR                                                    AS dimension_name,
  v:dimensions.dimension.mappingCriteria.mappingCriterion['@id']::VARCHAR                     AS mapping_criteria_id,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.dimension['@id']::VARCHAR           AS mapping_criteria_dimension_id,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.dimension['@type']::VARCHAR         AS mapping_criteria_dimension_type,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.dimension['@valueId']::VARCHAR      AS mapping_criteria_dimension_value_id,
  v:dimensions.dimension.mappingCriteria.mappingCriterion.mapTo['@dimensionValueId']::VARCHAR AS mapping_criteria_map_to_dimension_value_id,
  __loaded_at
FROM
  parsed
ORDER BY __loaded_at
