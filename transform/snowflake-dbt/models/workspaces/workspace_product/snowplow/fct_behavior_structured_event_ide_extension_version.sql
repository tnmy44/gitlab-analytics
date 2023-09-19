{{
  config(
    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=["mnpi_exception"]
  )
}}

WITH clicks AS (
  SELECT
    behavior_structured_event_pk,
    behavior_at,
    contexts
  FROM {{ ref('fct_behavior_structured_event') }}
  WHERE behavior_at >= '2023-08-01' -- no events added to context before Aug 2023
),

flattened AS (
  SELECT
    clicks.behavior_structured_event_pk,
    clicks.behavior_at,
    flat_contexts.value['data']['extension_name']::VARCHAR AS extension_name,
    flat_contexts.value['data']['extension_version']::VARCHAR AS extension_version,
    flat_contexts.value['data']['ide_name']::VARCHAR AS ide_name,
    flat_contexts.value['data']['ide_vendor']::VARCHAR AS ide_vendor,
    flat_contexts.value['data']['ide_version']::VARCHAR AS ide_version
  FROM clicks,
  LATERAL FLATTEN(input => TRY_PARSE_JSON(clicks.contexts), path => 'data') AS flat_contexts
  WHERE flat_contexts.value['schema']::VARCHAR = 'iglu:com.gitlab/ide_extension_version/jsonschema/1-0-0'
    {% if is_incremental() %}
    
        AND clicks.behavior_at >= (SELECT MAX(behavior_at) FROM {{this}})
    
    {% endif %}
)

{{ dbt_audit(
    cte_ref="flattened",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-09-19",
    updated_date="2023-09-19"
) }}
