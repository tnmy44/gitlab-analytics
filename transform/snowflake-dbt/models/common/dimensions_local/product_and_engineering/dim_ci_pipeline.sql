WITh prep AS (

    SELECT *  FROM {{ ref('prep_ci_pipeline')}}

), final AS (

    SELECT 
      dim_ci_pipeline_id, 

      -- FOREIGN KEYS
      dim_project_id,
      dim_namespace_id,
      ultimate_parent_namespace_id,
      dim_user_id,
      created_date_id,
      dim_plan_id,
      merge_request_id,

      created_at, 
      started_at, 
      committed_at,
      finished_at, 
      ci_pipeline_duration_in_s, 

      status, 
      ref,
      has_tag, 
      yaml_errors, 
      lock_version, 
      auto_canceled_by_id, 
      pipeline_schedule_id, 
      --- add enum from pipeline.rb
      CASE 
        WHEN ci_pipeline_source = 1 THEN 'push'
        WHEN ci_pipeline_source = 2 THEN 'web'
        WHEN ci_pipeline_source = 3 THEN 'trigger'
        WHEN ci_pipeline_source = 4 THEN 'schedule'
        WHEN ci_pipeline_source = 5 THEN 'api'
        WHEN ci_pipeline_source = 6 THEN 'external'
        WHEN ci_pipeline_source = 7 THEN 'pipeline'
        WHEN ci_pipeline_source = 8 THEN 'chat'
        WHEN ci_pipeline_source = 9 THEN 'webide'
        WHEN ci_pipeline_source = 10 THEN 'merge_request_event'
        WHEN ci_pipeline_source = 11 THEN 'external_pull_request_event'
        WHEN ci_pipeline_source = 12 THEN 'parent_pipeline'
        WHEN ci_pipeline_source = 13 THEN 'ondemand_dast_scan'
        WHEN ci_pipeline_source = 14 THEN 'ondemande_dast_validation'
        WHEN ci_pipeline_source = 15 THEN 'security_orchestration_policy'
        WHEN ci_pipeline_source = 16 THEN 'container_registry_push'
        ELSE NULL 
        END as ci_pipeline_source
      config_source, 
      is_protected, 
      failure_reason_id,
      failure_reason,
      ci_pipeline_internal_id,
      is_deleted,
      is_deleted_updated_at
    FROM prep

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mpeychet_",
    updated_by="@utkarsh060",
    created_date="2021-06-10",
    updated_date="2024-07-09"
) }}
