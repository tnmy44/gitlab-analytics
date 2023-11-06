{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('fct_behavior_structured_event_experiment','fct_behavior_structured_event_experiment'),
    ('dim_behavior_event', 'dim_behavior_event'),
	('snowplow_unnested_events_all_staging', 'snowplow_unnested_events_all_staging'),
	('prep_snowplow_gitlab_events_experiment_contexts_all', 'prep_snowplow_gitlab_events_experiment_contexts_all'),
	('dim_namespace', 'dim_namespace')
    ])
}},

experiment_events AS 
  
  ( --Most of the experiment events needed in staging and prod
  
    SELECT
      experiment.experiment_name,
      IFF(experiment.app_id IN ('gitlab','gitlab_customers'), 'production', 'staging')                     
                                                                AS dev_environment,
      experiment.app_id,
      experiment.experiment_variant,
      event_details.event_action,
      event_details.event_property,
      event_details.event_label,
      event_details.event_category,
      experiment.behavior_structured_event_pk                   AS event_id,
      experiment.gsc_pseudonymized_user_id,
      experiment.user_snowplow_domain_id,       
      experiment.dim_namespace_id,             
      experiment.context_key,                   
      experiment.behavior_at           
    FROM fct_behavior_structured_event_experiment AS experiment
    INNER JOIN dim_behavior_event AS event_details
      ON event_details.dim_behavior_event_sk = experiment.dim_behavior_event_sk
      AND experiment.behavior_at::DATE BETWEEN DATEADD(YEAR,-1,CURRENT_DATE()) and CURRENT_DATE() -- events triggered in the past 1 year for query efficiency
    

    UNION ALL

    
    SELECT -- Staging events
      experiment.experiment_name,
      IFF(stag.app_id IN ('gitlab','gitlab_customers'), 'production', 'staging')                     
                                                               AS dev_environment,
      stag.app_id,
      experiment.experiment_variant,
      stag.event_action,
      stag.event_property,
      stag.event_label,
      stag.event_category,
      stag.event_id,
      stag.gsc_pseudonymized_user_id,
      stag.user_snowplow_domain_id,
      stag.gsc_namespace_id,
      experiment.context_key,
      stag.behavior_at
    FROM snowplow_unnested_events_all_staging AS stag 
    INNER JOIN prep_snowplow_gitlab_events_experiment_contexts_all experiment 
      ON experiment.event_id = stag.event_id
    WHERE stag.has_gitlab_experiment_context = TRUE 
      AND stag.event_name = 'event'
      AND stag.behavior_at::DATE BETWEEN DATEADD(YEAR,-1,CURRENT_DATE()) and CURRENT_DATE() -- events triggered in the past 1 year for query efficiency

    ),

	base AS (

    SELECT 
      behavior_at::DATE                       AS behavior_date,
      dim_namespace.ultimate_parent_namespace_id,
      experiment_events.dim_namespace_id,
      COALESCE(experiment_events.dim_namespace_id::VARCHAR, context_key::VARCHAR)   
                                              AS entity_id, --context_key if namespace id is not present
      IFF(experiment_events.dim_namespace_id IS NULL, 'context_key', 'namespace')
                                              AS entity_category,
      dev_environment,
      app_id,
      experiment_name,
      experiment_variant,
      event_action,
      event_label,
      event_property,
      event_category,
      COUNT(DISTINCT gsc_pseudonymized_user_id) 
	                                          AS count_daily_gsc_pseudonymized_user_id,
      COUNT(DISTINCT user_snowplow_domain_id) AS count_daily_user_snowplow_domain_id,
      COUNT(DISTINCT event_id)                AS count_daily_events
    FROM experiment_events
    LEFT JOIN dim_namespace
      ON experiment_events.dim_namespace_id = dim_namespace.dim_namespace_id --not all experiments are set up to capture events at the top level namespace grain - joining namespace to include ultimate_parent_namespace_id
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13

	)

	{{ dbt_audit(
    cte_ref="base",
    created_by="@eneuberger",
    updated_by="@eneuberger",
    created_date="2023-10-23",
    updated_date="2023-10-23"
) }}