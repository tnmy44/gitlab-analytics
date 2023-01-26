WITH source AS (
  SELECT * FROM
    {{ source('adaptive', 'dimension_mapping') }}
),

parsed AS (
  SELECT
    PARSE_JSON(_data) AS dimension_mapping_data,
    __loaded_at
  FROM
    source
)

SELECT
  dimension_mapping_data['version']['@id']::VARCHAR                                                                   AS version_id,
  dimension_mapping_data['version']['@name']::VARCHAR                                                                 AS version_name,
  dimension_mapping_data['version']['@readOnly']::VARCHAR                                                             AS version_readonly,
  dimension_mapping_data['dimensions'].dimension['@id']::VARCHAR                                                      AS dimension_id,
  dimension_mapping_data['dimensions'].dimension['@mappingDimensionIds']::VARCHAR                                     AS mapping_dimension_ids,
  dimension_mapping_data['dimensions'].dimension['@name']::VARCHAR                                                    AS dimension_name,
  dimension_mapping_data['dimensions'].dimension.mappingCriteria.mappingCriterion['@id']::VARCHAR                     AS mapping_criteria_id,
  dimension_mapping_data['dimensions'].dimension.mappingCriteria.mappingCriterion.dimension['@id']::VARCHAR           AS mapping_criteria_dimension_id,
  dimension_mapping_data['dimensions'].dimension.mappingCriteria.mappingCriterion.dimension['@type']::VARCHAR         AS mapping_criteria_dimension_type,
  dimension_mapping_data['dimensions'].dimension.mappingCriteria.mappingCriterion.dimension['@valueId']::VARCHAR      AS mapping_criteria_dimension_value_id,
  dimension_mapping_data['dimensions'].dimension.mappingCriteria.mappingCriterion.mapTo['@dimensionValueId']::VARCHAR AS mapping_criteria_map_to_dimension_value_id,
  __loaded_at
FROM
  parsed
ORDER BY __loaded_at
