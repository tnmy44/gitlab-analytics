{{
  config(
    materialized='table',
    tags=["mnpi_exception"]
  )
}}

{{ simple_cte([
    ('paid_user_metrics', 'mart_product_usage_paid_user_metrics_monthly'),
    ('dim_crm_account', 'dim_crm_account'),
    ('mart_arr', 'mart_arr')
])}},

joined AS (

  SELECT
    paid_user_metrics.snapshot_month,
    paid_user_metrics.primary_key,
    dim_crm_account.crm_account_name,
    paid_user_metrics.dim_crm_account_id,
    dim_crm_account.customer_since_date,
    DATEDIFF(MONTH, dim_crm_account.customer_since_date, paid_user_metrics.snapshot_month)                                                                      AS account_age_months,
    DATEDIFF(MONTH, paid_user_metrics.subscription_start_date, paid_user_metrics.snapshot_month)                                                                AS subscription_age_months,
    COALESCE(paid_user_metrics.installation_creation_date, paid_user_metrics.namespace_creation_date)                                                           AS combined_instance_creation_date,
    DATEDIFF(DAY, combined_instance_creation_date, paid_user_metrics.ping_created_at)                                                                           AS instance_age_days,
    DATEDIFF(MONTH, combined_instance_creation_date, paid_user_metrics.ping_created_at)                                                                         AS instance_age_months,
    paid_user_metrics.subscription_start_date,
    paid_user_metrics.subscription_name,
    paid_user_metrics.dim_subscription_id_original,
    paid_user_metrics.dim_subscription_id,
    paid_user_metrics.delivery_type,
    paid_user_metrics.instance_type,
    paid_user_metrics.included_in_health_measures_str,
    paid_user_metrics.uuid,
    paid_user_metrics.hostname,
    paid_user_metrics.dim_namespace_id,
    paid_user_metrics.dim_installation_id,
    IFF(paid_user_metrics.dim_namespace_id IS NULL, paid_user_metrics.dim_installation_id, paid_user_metrics.dim_namespace_id)                                  AS instance_identifier,
    COALESCE(paid_user_metrics.hostname, paid_user_metrics.dim_namespace_id)                                                                                    AS hostname_or_namespace_id,
    paid_user_metrics.ping_created_at,
    paid_user_metrics.cleaned_version,

    -- license utilization metrics --
    paid_user_metrics.license_utilization,
    paid_user_metrics.billable_user_count,
    paid_user_metrics.license_user_count,
    paid_user_metrics.license_user_count_source,
    (CASE WHEN account_age_months < 3 THEN NULL
        WHEN account_age_months >= 3 AND account_age_months < 7 THEN (CASE WHEN paid_user_metrics.license_utilization <= .10 THEN 25
          WHEN paid_user_metrics.license_utilization > .10 AND paid_user_metrics.license_utilization <= .50 THEN 63
          WHEN paid_user_metrics.license_utilization > .50 THEN 88 END)
        WHEN account_age_months >= 7 AND account_age_months < 10 THEN (CASE WHEN paid_user_metrics.license_utilization <= .50 THEN 25
          WHEN paid_user_metrics.license_utilization > .50 AND paid_user_metrics.license_utilization <= .75 THEN 63
          WHEN paid_user_metrics.license_utilization > .75 THEN 88 END)
        WHEN account_age_months >= 10 THEN (CASE WHEN paid_user_metrics.license_utilization <= .75 THEN 25
          WHEN paid_user_metrics.license_utilization > .75 THEN 88 END)
      END)                                                                                                                                                      AS license_utilization_score,
    CASE WHEN license_utilization_score IS NULL THEN NULL
      WHEN license_utilization_score = 25 THEN 'Red'
      WHEN license_utilization_score = 63 THEN 'Yellow'
      WHEN license_utilization_score = 88 THEN 'Green' END                                                                                                      AS license_utilization_color,

    -- user engagement metrics --
    paid_user_metrics.last_activity_28_days_user,
    CASE WHEN paid_user_metrics.delivery_type = 'SaaS' THEN NULL
      WHEN paid_user_metrics.delivery_type = 'Self-Managed' THEN DIV0(paid_user_metrics.last_activity_28_days_user, paid_user_metrics.billable_user_count) END  AS user_engagement,
    CASE WHEN user_engagement IS NULL THEN NULL
      WHEN user_engagement < .50 THEN 25
      WHEN user_engagement >= .50 AND user_engagement < .80 THEN 63
      WHEN user_engagement >= .80 THEN 88 END                                                                                                                   AS user_engagement_score,
    CASE WHEN user_engagement_score IS NULL THEN NULL
      WHEN user_engagement_score = 25 THEN 'Red'
      WHEN user_engagement_score = 63 THEN 'Yellow'
      WHEN user_engagement_score = 88 THEN 'Green' END                                                                                                          AS user_engagement_color,

    -- scm metrics --
    CASE WHEN instance_identifier IS NULL THEN NULL ELSE paid_user_metrics.action_monthly_active_users_project_repo_28_days_user END                            AS action_monthly_active_users_project_repo_28_days_user_clean,
    DIV0(action_monthly_active_users_project_repo_28_days_user_clean, paid_user_metrics.billable_user_count)                                                    AS git_operation_utilization,
    CASE WHEN git_operation_utilization IS NULL THEN NULL
      WHEN git_operation_utilization < .25 THEN 25
      WHEN git_operation_utilization >= .25 AND git_operation_utilization < .50 THEN 63
      WHEN git_operation_utilization >= .50 THEN 88 END                                                                                                         AS scm_score,
    CASE WHEN scm_score IS NULL THEN NULL
      WHEN scm_score = 25 THEN 'Red'
      WHEN scm_score = 63 THEN 'Yellow'
      WHEN scm_score = 88 THEN 'Green' END                                                                                                                      AS scm_color,

    -- ci metrics --
    paid_user_metrics.ci_pipelines_28_days_user,
    DIV0(paid_user_metrics.ci_pipelines_28_days_user, paid_user_metrics.billable_user_count)                                                                    AS ci_pipeline_utilization,
    CASE WHEN ci_pipeline_utilization IS NULL THEN NULL
      WHEN ci_pipeline_utilization < .25 THEN 25
      WHEN ci_pipeline_utilization >= .25 AND ci_pipeline_utilization < .50 THEN 63
      WHEN ci_pipeline_utilization >= .50 THEN 88 END                                                                                                           AS ci_pipeline_utilization_score,
    CASE WHEN ci_pipeline_utilization_score IS NULL THEN NULL
      WHEN ci_pipeline_utilization_score = 25 THEN 'Red'
      WHEN ci_pipeline_utilization_score = 63 THEN 'Yellow'
      WHEN ci_pipeline_utilization_score = 88 THEN 'Green' END                                                                                                  AS ci_pipeline_utilization_color,
    ci_pipeline_utilization_score                                                                                                                               AS ci_score,
    CASE WHEN ci_score IS NULL THEN NULL
      WHEN ci_score = 25 THEN 'Red'
      WHEN ci_score = 63 THEN 'Yellow'
      WHEN ci_score = 88 THEN 'Green' END                                                                                                                       AS ci_color,

    -- cd metrics --
    paid_user_metrics.deployments_28_days_user,
    paid_user_metrics.deployments_28_days_event,
    paid_user_metrics.successful_deployments_28_days_event,
    paid_user_metrics.failed_deployments_28_days_event,
    (paid_user_metrics.successful_deployments_28_days_event + paid_user_metrics.failed_deployments_28_days_event)                                               AS completed_deployments_l28d,
    paid_user_metrics.projects_all_time_event,
    paid_user_metrics.environments_all_time_event,
    DIV0(paid_user_metrics.deployments_28_days_user, paid_user_metrics.billable_user_count)                                                                     AS deployments_utilization,
    CASE WHEN deployments_utilization IS NULL THEN NULL
      WHEN deployments_utilization < .05 THEN 25
      WHEN deployments_utilization >= .05 AND deployments_utilization <= .12 THEN 63
      WHEN deployments_utilization > .12 THEN 88 END                                                                                                            AS deployments_utilization_score,
    CASE WHEN deployments_utilization_score IS NULL THEN NULL
      WHEN deployments_utilization_score = 25 THEN 'Red'
      WHEN deployments_utilization_score = 63 THEN 'Yellow'
      WHEN deployments_utilization_score = 88 THEN 'Green' END                                                                                                  AS deployments_utilization_color,
    DIV0(paid_user_metrics.deployments_28_days_event, paid_user_metrics.billable_user_count)                                                                    AS deployments_per_user_l28d,
    CASE WHEN deployments_per_user_l28d IS NULL THEN NULL
      WHEN deployments_per_user_l28d < 2 THEN 25
      WHEN deployments_per_user_l28d >= 2 AND deployments_per_user_l28d <= 7 THEN 63
      WHEN deployments_per_user_l28d > 7 THEN 88 END                                                                                                            AS deployments_per_user_l28d_score,
    CASE WHEN deployments_per_user_l28d_score IS NULL THEN NULL
      WHEN deployments_per_user_l28d_score = 25 THEN 'Red'
      WHEN deployments_per_user_l28d_score = 63 THEN 'Yellow'
      WHEN deployments_per_user_l28d_score = 88 THEN 'Green' END                                                                                                AS deployments_per_user_l28d_color,
    DIV0(successful_deployments_28_days_event, completed_deployments_l28d)                                                                                      AS successful_deployments_pct,
    CASE WHEN successful_deployments_pct IS NULL THEN NULL
      WHEN successful_deployments_pct < .25 THEN 25
      WHEN successful_deployments_pct >= .25 AND successful_deployments_pct <= .80 THEN 63
      WHEN successful_deployments_pct > .80 THEN 88 END                                                                                                         AS successful_deployments_pct_score,
    CASE WHEN successful_deployments_pct_score IS NULL THEN NULL
      WHEN successful_deployments_pct_score = 25 THEN 'Red'
      WHEN successful_deployments_pct_score = 63 THEN 'Yellow'
      WHEN successful_deployments_pct_score = 88 THEN 'Green' END                                                                                               AS successful_deployments_pct_color,
    IFF(deployments_utilization_score IS NOT NULL, 1, 0)
    + IFF(deployments_per_user_l28d_score IS NOT NULL, 1, 0)
    + IFF(successful_deployments_pct_score IS NOT NULL, 1, 0)                                                                                                   AS cd_measure_count,
    IFF(cd_measure_count = 0, NULL, DIV0((ZEROIFNULL(deployments_utilization_score)
          + ZEROIFNULL(deployments_per_user_l28d_score)
          + ZEROIFNULL(successful_deployments_pct_score)), cd_measure_count))                                                                                   AS cd_score,
    IFF(cd_measure_count = 0, NULL, (CASE WHEN cd_score <= 50 THEN 'Red'
          WHEN cd_score > 50 AND cd_score <= 75 THEN 'Yellow'
          WHEN cd_score > 75 THEN 'Green' END))                                                                                                                 AS cd_color,

    -- security metrics --
    paid_user_metrics.user_unique_users_all_secure_scanners_28_days_user,
    paid_user_metrics.user_container_scanning_jobs_28_days_user,
    paid_user_metrics.user_secret_detection_jobs_28_days_user,
    DIV0(paid_user_metrics.user_unique_users_all_secure_scanners_28_days_user, paid_user_metrics.billable_user_count)                                           AS secure_scanners_utilization,
    CASE WHEN secure_scanners_utilization IS NULL THEN NULL
      WHEN secure_scanners_utilization <= .05 THEN 25
      WHEN secure_scanners_utilization > .05 AND secure_scanners_utilization < .20 THEN 63
      WHEN secure_scanners_utilization >= .20 THEN 88 END                                                                                                       AS secure_scanners_utilization_score,
    CASE WHEN secure_scanners_utilization_score IS NULL THEN NULL
      WHEN secure_scanners_utilization_score = 25 THEN 'Red'
      WHEN secure_scanners_utilization_score = 63 THEN 'Yellow'
      WHEN secure_scanners_utilization_score = 88 THEN 'Green' END                                                                                              AS secure_scanners_utilization_color,
    DIV0(paid_user_metrics.user_container_scanning_jobs_28_days_user, paid_user_metrics.billable_user_count)                                                    AS container_scanning_jobs_utilization,
    CASE WHEN container_scanning_jobs_utilization IS NULL THEN NULL
      WHEN container_scanning_jobs_utilization <= .03 THEN 25
      WHEN container_scanning_jobs_utilization > .03 AND container_scanning_jobs_utilization < .10 THEN 63
      WHEN container_scanning_jobs_utilization >= .10 THEN 88 END                                                                                               AS container_scanning_jobs_utilization_score,
    CASE WHEN container_scanning_jobs_utilization_score IS NULL THEN NULL
      WHEN container_scanning_jobs_utilization_score = 25 THEN 'Red'
      WHEN container_scanning_jobs_utilization_score = 63 THEN 'Yellow'
      WHEN container_scanning_jobs_utilization_score = 88 THEN 'Green' END                                                                                      AS container_scanning_jobs_utilization_color,
    DIV0(paid_user_metrics.user_secret_detection_jobs_28_days_user, paid_user_metrics.billable_user_count)                                                      AS secret_detection_jobs_utilization,
    CASE WHEN secret_detection_jobs_utilization IS NULL THEN NULL
      WHEN secret_detection_jobs_utilization <= .06 THEN 25
      WHEN secret_detection_jobs_utilization > .06 AND secret_detection_jobs_utilization < .20 THEN 63
      WHEN secret_detection_jobs_utilization >= .20 THEN 88 END                                                                                                 AS secret_detection_jobs_utilization_score,
    CASE WHEN secret_detection_jobs_utilization_score IS NULL THEN NULL
      WHEN secret_detection_jobs_utilization_score = 25 THEN 'Red'
      WHEN secret_detection_jobs_utilization_score = 63 THEN 'Yellow'
      WHEN secret_detection_jobs_utilization_score = 88 THEN 'Green' END                                                                                        AS secret_detection_jobs_utilization_color,
    IFF(secure_scanners_utilization_score IS NOT NULL, 1, 0)
    + IFF(container_scanning_jobs_utilization_score IS NOT NULL, 1, 0)
    + IFF(secret_detection_jobs_utilization_score IS NOT NULL, 1, 0)                                                                                            AS security_measure_count,
    IFF(security_measure_count = 0, NULL, DIV0((ZEROIFNULL(secure_scanners_utilization_score)
          + ZEROIFNULL(container_scanning_jobs_utilization_score)
          + ZEROIFNULL(secret_detection_jobs_utilization_score)), security_measure_count))                                                                      AS security_score,
    IFF(security_measure_count = 0, NULL, CASE WHEN security_score <= 50 THEN 'Red'
        WHEN security_score > 50 AND security_score <= 75 THEN 'Yellow'
        WHEN security_score > 75 THEN 'Green' END)                                                                                                              AS security_color,
    IFF(mart_arr.product_tier_name LIKE '%Ultimate%', security_score, NULL)                                                                                     AS security_score_ultimate_only,
    IFF(mart_arr.product_tier_name LIKE '%Ultimate%', security_color, NULL)                                                                                     AS security_color_ultimate_only,

    -- overall product score --
    IFF(license_utilization_score IS NULL, 0, .30)                                                                                                              AS license_utilization_weight,
    IFF(user_engagement_score IS NULL, 0, .10)                                                                                                                  AS user_engagement_weight,
    IFF(scm_score IS NULL, 0, .15)                                                                                                                              AS scm_weight,
    IFF(ci_score IS NULL, 0, .15)                                                                                                                               AS ci_weight,
    IFF(cd_score IS NULL, 0, .15)                                                                                                                               AS cd_weight,
    IFF(security_score_ultimate_only IS NULL, 0, .15)                                                                                                           AS security_weight,
    license_utilization_weight + user_engagement_weight + scm_weight + ci_weight + cd_weight + security_weight                                                  AS remaining_weight,
    DIV0(license_utilization_weight, remaining_weight)                                                                                                          AS adjusted_license_utilization_weight,
    DIV0(user_engagement_weight, remaining_weight)                                                                                                              AS adjusted_user_engagement_weight,
    DIV0(scm_weight, remaining_weight)                                                                                                                          AS adjusted_scm_weight,
    DIV0(ci_weight, remaining_weight)                                                                                                                           AS adjusted_ci_weight,
    DIV0(cd_weight, remaining_weight)                                                                                                                           AS adjusted_cd_weight,
    DIV0(security_weight, remaining_weight)                                                                                                                     AS adjusted_security_weight,
    ZEROIFNULL(license_utilization_score) * adjusted_license_utilization_weight                                                                                 AS adjusted_license_utilization_score,
    ZEROIFNULL(user_engagement_score) * adjusted_user_engagement_weight                                                                                         AS adjusted_user_engagement_score,
    ZEROIFNULL(scm_score) * adjusted_scm_weight                                                                                                                 AS adjusted_scm_score,
    ZEROIFNULL(ci_score) * adjusted_ci_weight                                                                                                                   AS adjusted_ci_score,
    ZEROIFNULL(cd_score) * adjusted_cd_weight                                                                                                                   AS adjusted_cd_score,
    ZEROIFNULL(security_score_ultimate_only) * adjusted_security_weight                                                                                         AS adjusted_security_score,
    adjusted_license_utilization_score + adjusted_user_engagement_score + adjusted_scm_score + adjusted_ci_score + adjusted_cd_score + adjusted_security_score  AS overall_product_score,
    CASE WHEN overall_product_score <= 50 THEN 'Red'
      WHEN overall_product_score > 50 AND overall_product_score <= 75 THEN 'Yellow'
      WHEN overall_product_score > 75 THEN 'Green' END                                                                                                          AS overall_product_color,
    IFF(scm_color IS NULL, NULL, IFF(scm_color = 'Green', 1, 0))                                                                                                AS scm_adopted,
    IFF(ci_color IS NULL, NULL, IFF(ci_color = 'Green', 1, 0))                                                                                                  AS ci_adopted,
    IFF(cd_color IS NULL, NULL, IFF(cd_color = 'Green', 1, 0))                                                                                                  AS cd_adopted,
    IFF(security_color_ultimate_only IS NULL, NULL, IFF(security_color_ultimate_only = 'Green', 1, 0))                                                          AS security_adopted,
    IFF(scm_adopted IS NULL
      AND ci_adopted IS NULL
      AND cd_adopted IS NULL
      AND security_color_ultimate_only IS NULL, NULL, ZEROIFNULL(scm_adopted) + ZEROIFNULL(ci_adopted) + ZEROIFNULL(cd_adopted) + ZEROIFNULL(security_adopted)) AS total_use_cases_adopted,
    ARRAY_CONSTRUCT_COMPACT(
      IFF(scm_adopted = 1, 'SCM', NULL),
      IFF(ci_adopted = 1, 'CI', NULL),
      IFF(cd_adopted = 1, 'CD', NULL),
      IFF(security_adopted = 1, 'Security', NULL)
    )                                                                                                                                                           AS adopted_use_case_names_array,
    ARRAY_TO_STRING(adopted_use_case_names_array, ', ')                                                                                                         AS adopted_use_case_names_string
  FROM paid_user_metrics
  LEFT JOIN dim_crm_account
    ON paid_user_metrics.dim_crm_account_id = dim_crm_account.dim_crm_account_id
  LEFT JOIN mart_arr
    ON paid_user_metrics.dim_subscription_id_original = mart_arr.dim_subscription_id_original
      AND paid_user_metrics.snapshot_month = mart_arr.arr_month
  WHERE paid_user_metrics.license_user_count != 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY paid_user_metrics.snapshot_month, instance_identifier ORDER BY paid_user_metrics.ping_created_at DESC NULLS LAST) = 1

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@jngCES",
    updated_by="@jpeguero",
    created_date="2023-03-30",
    updated_date="2023-06-08"
) }}
