SELECT
  PARSE_JSON(_data):"@id"::varchar AS run_id,
  version.value['@id']::varchar,
  version.value['@name']::varchar,
  version.value['@shortName']::varchar,
  version.value['@type']::varchar,
  version.value['@isVirtual']::varchar,
  version.value['@description']::varchar,
  version.value['@isDefaultVersion']::varchar,
  version.value['@isLocked']::varchar,
  version.value['@hasAuditTrail']::varchar,
  version.value['@enabledForWorkflow']::varchar,
  version.value['@isImportable']::varchar,
  version.value['@isPredictive']::varchar,
  version.value['@leftScroll']::varchar,
  version.value['@startPlan']::varchar,
  version.value['@endPlan']::varchar,
  version.value['@lockLeading'],
  __LOADED_AT
FROM
  "RAW"."TAP_ADAPTIVE"."VERSIONS",
  LATERAL FLATTEN(input => PARSE_JSON(_data):version) version
  -- lateral flatten(input => version.value, recursive => True) version_fields
ORDER BY
  __LOADED_AT;
