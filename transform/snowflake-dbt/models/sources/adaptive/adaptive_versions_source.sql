WITH source AS (
  SELECT * FROM
    {{ source('adaptive', 'versions') }}
)

SELECT
  PARSE_JSON(_data) :"@id"::VARCHAR             AS parent_id,
  version.value['@id']::VARCHAR                 AS id,
  version.value['@name']::VARCHAR               AS name,
  version.value['@shortName']::VARCHAR          AS short_name,
  version.value['@type']::VARCHAR               AS version_type,
  version.value['@isVirtual']::VARCHAR          AS is_virtual,
  version.value['@description']::VARCHAR        AS description,
  version.value['@isDefaultVersion']::VARCHAR   AS is_default_version,
  version.value['@isLocked']::VARCHAR           AS is_locked,
  version.value['@hasAuditTrail']::VARCHAR      AS has_audit_trail,
  version.value['@enabledForWorkflow']::VARCHAR AS enabled_for_workflow,
  version.value['@isImportable']::VARCHAR       AS is_importable,
  version.value['@isPredictive']::VARCHAR       AS is_predictive,
  version.value['@leftScroll']::VARCHAR         AS left_scroll,
  version.value['@startPlan']::VARCHAR          AS start_plan,
  version.value['@endPlan']::VARCHAR            AS end_plan,
  version.value['@lockLeading']                 AS lock_leading,
  __loaded_at
FROM
  source,
  LATERAL FLATTEN(input => PARSE_JSON(_data) ['version']) version
ORDER BY
  __loaded_at
