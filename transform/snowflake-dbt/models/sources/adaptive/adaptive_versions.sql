SELECT
  PARSE_JSON(_data):"@id"::varchar AS parent_id,
  version.value['@id']::varchar AS id,
  version.value['@name']::varchar AS name,
  version.value['@shortName']::varchar AS short_name,
  version.value['@type']::varchar AS version_type,
  version.value['@isVirtual']::varchar AS is_virtual,
  version.value['@description']::varchar AS description,
  version.value['@isDefaultVersion']::varchar AS is_default_version,
  version.value['@isLocked']::varchar AS is_locked,
  version.value['@hasAuditTrail']::varchar AS has_audit_trail,
  version.value['@enabledForWorkflow']::varchar AS enabled_for_workflow,
  version.value['@isImportable']::varchar AS is_importable,
  version.value['@isPredictive']::varchar AS is_predictive,
  version.value['@leftScroll']::varchar AS left_scroll,
  version.value['@startPlan']::varchar AS start_plan,
  version.value['@endPlan']::varchar AS end_plan,
  version.value['@lockLeading'] AS lock_leading,
  __LOADED_AT
FROM
  "RAW"."TAP_ADAPTIVE"."VERSIONS",
  LATERAL FLATTEN(input => PARSE_JSON(_data):version) version
ORDER BY
  __LOADED_AT
