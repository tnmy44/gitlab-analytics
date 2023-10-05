{{ config({
        "materialized": "table",
        "unique_key": "user_reporting_month_pk",
        "tags": ["product", "mnpi_exception"]
    })
}}

SELECT
    {{ dbt_utils.surrogate_key(['reporting_month', 'dim_user_id']) }} AS user_reporting_month_pk,
    DATE_TRUNC('month',ci_build_started_at)::DATE AS reporting_month,
    fct_ci_runner_activity.dim_user_id,
    ultimate_parent_namespace_id,
    dim_project_id, 
    plan_name_at_event_month,
    CASE
      WHEN ci_runner_type_summary = 'shared' THEN 'Shared Runners'
      ELSE 'Self-Managed Runners'
    END AS runner_type,
    ci_runner_manager,
    ci_runner_machine_type,
    COUNT(DISTINCT fct_ci_runner_activity.dim_ci_runner_id) as count_of_runners,
    COUNT(DISTINCT dim_ci_pipeline_id) as count_of_pipelines,
    SUM(ci_build_duration_in_s) / 60 AS ci_build_minutes
  FROM common.fct_ci_runner_activity as fct_ci_runner_activity
  JOIN common.dim_ci_runner
    ON fct_ci_runner_activity.dim_ci_runner_id = dim_ci_runner.dim_ci_runner_id
  JOIN workspace_product.wk_rpt_event_namespace_plan_monthly as namespace_plan_monthly
    ON fct_ci_runner_activity.ultimate_parent_namespace_id = namespace_plan_monthly.dim_ultimate_parent_namespace_id
    AND reporting_month = namespace_plan_monthly.event_calendar_month
  WHERE ci_build_started_at >= '2021-01-01'
    AND ci_build_finished_at IS NOT NULL
    AND namespace_is_internal = FALSE
    AND namespace_creator_is_blocked = FALSE
  {{ dbt_utils.group_by(n=8) }}
