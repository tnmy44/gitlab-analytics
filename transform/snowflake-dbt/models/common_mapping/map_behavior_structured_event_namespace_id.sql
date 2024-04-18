{{config(

    materialized='incremental',
    unique_key=['behavior_structured_event_pk','namespace_id'],
    tags=['product'],
    full_refresh= only_force_full_refresh(),
    on_schema_change='sync_all_columns',
    post_hook=["{{ rolling_window_delete('behavior_at','month',25) }}"]
  )

}}

{{ simple_cte([
    ('prep_snowplow_unnested_events_all', 'prep_snowplow_unnested_events_all')
]) }}

, flattened_namespaces_standard_context AS (

  SELECT 
    prep_snowplow_unnested_events_all.event_id ,
    flattened_namespace.value::VARCHAR                                 AS namespace_id
  FROM prep_snowplow_unnested_events_all,
  LATERAL FLATTEN (input => TRY_PARSE_JSON(prep_snowplow_unnested_events_all.gsc_feature_enabled_by_namespace_ids)) AS flattened_namespace
  WHERE gsc_feature_enabled_by_namespace_ids IS NOT NULL
    AND gsc_feature_enabled_by_namespace_ids != '[]'

)

, flattened_namespaces_code_suggestions AS (

  SELECT 
    prep_snowplow_unnested_events_all.event_id ,
    flattened_namespace.value::VARCHAR                                 AS namespace_id
  FROM prep_snowplow_unnested_events_all,
  LATERAL FLATTEN (input => TRY_PARSE_JSON(prep_snowplow_unnested_events_all.namespace_ids)) AS flattened_namespace
  WHERE namespace_ids IS NOT NULL
    AND namespace_ids != '[]'

), unioned_namespaces AS (

  SELECT *
  FROM flattened_namespaces_standard_context

  UNION

  SELECT *
  FROM flattened_namespaces_code_suggestions

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2024-04-16",
    updated_date="2024-04-16"
) }}