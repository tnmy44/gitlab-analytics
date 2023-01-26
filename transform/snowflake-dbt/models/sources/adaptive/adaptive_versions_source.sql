WITH source AS (
  SELECT * FROM
    {{ source('adaptive', 'versions') }}
)

SELECT
  PARSE_JSON(source._data) :"@id"::VARCHAR       AS parent_id,
  versions.value['@id']::VARCHAR                 AS id,
  versions.value['@name']::VARCHAR               AS version_name,
  versions.value['@shortName']::VARCHAR          AS short_name,
  versions.value['@type']::VARCHAR               AS version_type,
  versions.value['@isVirtual']::VARCHAR          AS is_virtual,
  versions.value['@description']::VARCHAR        AS description,
  versions.value['@isDefaultVersion']::VARCHAR   AS is_default_version,
  versions.value['@isLocked']::VARCHAR           AS is_locked,
  versions.value['@hasAuditTrail']::VARCHAR      AS has_audit_trail,
  versions.value['@enabledForWorkflow']::VARCHAR AS enabled_for_workflow,
  versions.value['@isImportable']::VARCHAR       AS is_importable,
  versions.value['@isPredictive']::VARCHAR       AS is_predictive,
  versions.value['@leftScroll']::VARCHAR         AS left_scroll,
  versions.value['@startPlan']::VARCHAR          AS start_plan,
  versions.value['@endPlan']::VARCHAR            AS end_plan,
  versions.value['@lockLeading']                 AS lock_leading,
  source.__loaded_at
FROM
  source,
  LATERAL FLATTEN(input => PARSE_JSON(source._data) ['version']) AS versions
ORDER BY
  source.__loaded_at
