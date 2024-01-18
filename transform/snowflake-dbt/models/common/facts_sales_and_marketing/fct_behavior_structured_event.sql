{{config(

    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=['product'],
    full_refresh= only_force_full_refresh(),
    on_schema_change='sync_all_columns',
    post_hook=["{{ rolling_window_delete('behavior_at','month',25) }}"],
    cluster_by=['behavior_at::DATE']
  )

}}

WITH plans AS (
  SELECT *
  FROM {{ ref('prep_gitlab_dotcom_plan') }}
),

structured_event_renamed AS (
    SELECT
    
      event_id,
      tracker_version,
      dim_behavior_event_sk,
      event_value,
      contexts,
      dvce_created_tstamp,
      behavior_at,
      user_snowplow_domain_id,
      session_id,
      session_index,
      platform,
      page_url_host_path,
      page_url_scheme,
      page_url_host,
      page_url_path,
      clean_url_path,
      page_url_fragment,
      app_id,
      dim_behavior_browser_sk,
      dim_behavior_operating_system_sk,
      dim_behavior_website_page_sk,
      dim_behavior_referrer_page_sk,
      gitlab_standard_context,
      gsc_environment,
      gsc_extra,
      gsc_namespace_id,
      gsc_plan,
      gsc_google_analytics_client_id,
      gsc_project_id,
      gsc_pseudonymized_user_id,
      gsc_source,
      user_city, 
      user_country,
      user_region,
      user_timezone_name,
      has_performance_timing_context, 
      has_web_page_context,
      has_ci_build_failed_context,
      has_wiki_page_context,
      has_gitlab_standard_context,
      has_email_campaigns_context,
      has_gitlab_service_ping_context,
      has_design_management_context,
      has_customer_standard_context,
      has_secure_scan_context,
      has_gitlab_experiment_context, 
      has_subscription_auto_renew_context,
      has_code_suggestions_context,
      has_ide_extension_version_context,
      ide_extension_version_context,
      extension_name,
      extension_version,
      ide_name,
      ide_vendor,
      ide_version,
      language_server_version,
      experiment_context,
      experiment_name,
      experiment_context_key,
      experiment_variant,
      experiment_migration_keys,
      code_suggestions_context,
      model_engine,
      model_name,
      prefix_length,
      suffix_length,
      language,
      user_agent,
      delivery_type,
      api_status_code,
      namespace_ids,
      instance_id,
      host_name,
      service_ping_context,
      redis_event_name,
      key_path,
      data_source

    FROM {{ ref('prep_snowplow_unnested_events_all') }}
    WHERE event = 'struct'
    {% if is_incremental() %}

      AND behavior_at > (SELECT MAX({{ var('incremental_backfill_date', 'behavior_at') }}) FROM {{ this }})
      AND behavior_at <= (SELECT DATEADD(month, 1,  MAX({{ var('incremental_backfill_date', 'behavior_at') }}) )  FROM {{ this }})


    {% endif %}

), 

events_with_plan AS (
  SELECT 
    structured_event_renamed.*,
    plans.dim_plan_id,
    plans.dim_plan_sk
  FROM structured_event_renamed
  LEFT JOIN plans
    ON structured_event_renamed.gsc_plan = plans.plan_name
),

structured_events_w_dim AS (

    SELECT

      -- Primary Key
      events_with_plan.event_id                             AS behavior_structured_event_pk,

      -- Foreign Keys
      events_with_plan.dim_behavior_website_page_sk,
      events_with_plan.dim_behavior_referrer_page_sk,
      events_with_plan.dim_behavior_browser_sk,
      events_with_plan.dim_behavior_operating_system_sk,
      events_with_plan.gsc_namespace_id                     AS dim_namespace_id,
      events_with_plan.gsc_project_id                       AS dim_project_id,
      events_with_plan.dim_behavior_event_sk,
      {{ get_keyed_nulls('events_with_plan.dim_plan_sk') }} AS dim_plan_sk,
      events_with_plan.dim_plan_id,

      -- Time Attributes
      events_with_plan.dvce_created_tstamp,
      events_with_plan.behavior_at,

      -- Degenerate Dimensions (Event Attributes)
      events_with_plan.tracker_version,
      events_with_plan.session_index,
      events_with_plan.app_id,
      events_with_plan.session_id,
      events_with_plan.user_snowplow_domain_id,
      events_with_plan.contexts,
      events_with_plan.page_url_host_path,
      events_with_plan.page_url_path,
      events_with_plan.page_url_scheme,
      events_with_plan.page_url_host,
      events_with_plan.page_url_fragment,
      events_with_plan.event_value,

      -- Degenerate Dimensions (Gitlab Standard Context Attributes)
      events_with_plan.gitlab_standard_context,
      events_with_plan.gsc_google_analytics_client_id,
      events_with_plan.gsc_pseudonymized_user_id,
      events_with_plan.gsc_environment,
      events_with_plan.gsc_extra,
      events_with_plan.gsc_plan,
      events_with_plan.gsc_source,

      -- Degenerate Dimensions (IDE Extension Version Context Attributes)
      events_with_plan.ide_extension_version_context,
      events_with_plan.extension_name,
      events_with_plan.extension_version,
      events_with_plan.ide_name,
      events_with_plan.ide_vendor,
      events_with_plan.ide_version,
      events_with_plan.language_server_version,

      -- Degenerate Dimensions (User Location)
      events_with_plan.user_city, 
      events_with_plan.user_country,
      events_with_plan.user_region,
      events_with_plan.user_timezone_name,

      -- Degenerate Dimensions (Experiment)
      events_with_plan.experiment_context,
      events_with_plan.experiment_name,
      events_with_plan.experiment_context_key,
      events_with_plan.experiment_variant,
      events_with_plan.experiment_migration_keys,

      -- Degenerate Dimensions (Code Suggestions)
      events_with_plan.code_suggestions_context,
      events_with_plan.model_engine,
      events_with_plan.model_name,
      events_with_plan.prefix_length,
      events_with_plan.suffix_length,
      events_with_plan.language,
      events_with_plan.user_agent,
      events_with_plan.delivery_type,
      events_with_plan.api_status_code,
      events_with_plan.namespace_ids,
      events_with_plan.instance_id,
      events_with_plan.host_name,

      -- Degenerate Dimensions (Service Ping)
      events_with_plan.service_ping_context,
      events_with_plan.redis_event_name,
      events_with_plan.key_path,
      events_with_plan.data_source,

      -- Junk Dimensions (Context Flags)
      events_with_plan.has_performance_timing_context, 
      events_with_plan.has_web_page_context,
      events_with_plan.has_ci_build_failed_context,
      events_with_plan.has_wiki_page_context,
      events_with_plan.has_gitlab_standard_context,
      events_with_plan.has_email_campaigns_context,
      events_with_plan.has_gitlab_service_ping_context,
      events_with_plan.has_design_management_context,
      events_with_plan.has_customer_standard_context,
      events_with_plan.has_secure_scan_context,
      events_with_plan.has_gitlab_experiment_context, 
      events_with_plan.has_subscription_auto_renew_context,
      events_with_plan.has_code_suggestions_context,
      events_with_plan.has_ide_extension_version_context

    FROM events_with_plan

)

{{ dbt_audit(
    cte_ref="structured_events_w_dim",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-09-01",
    updated_date="2024-01-18"
) }}
