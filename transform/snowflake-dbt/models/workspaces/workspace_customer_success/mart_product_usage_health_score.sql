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
])}}

, joined AS (
    
    SELECT
        paid_user_metrics.snapshot_month,
        paid_user_metrics.primary_key,
        dim_crm_account.crm_account_name,
        paid_user_metrics.dim_crm_account_id,
        dim_crm_account.customer_since_date,
    --     ,months_between(snapshot_month, customer_since_date)
        DATEDIFF(MONTH, dim_crm_account.customer_since_date, paid_user_metrics.snapshot_month)                       AS account_age_months,
        DATEDIFF(MONTH, paid_user_metrics.subscription_start_date, paid_user_metrics.snapshot_month)                   AS subscription_age_months,
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
        IFF(paid_user_metrics.dim_namespace_id IS NULL, paid_user_metrics.dim_installation_id, paid_user_metrics.dim_namespace_id)    AS instance_identifier,
        paid_user_metrics.ping_created_at,
        paid_user_metrics.cleaned_version,

-- license utilization metrics --
        paid_user_metrics.license_utilization,
        paid_user_metrics.billable_user_count,
        paid_user_metrics.license_user_count,
        paid_user_metrics.license_user_count_source,
        (CASE WHEN account_age_months < 3 THEN NULL
        WHEN account_age_months >= 3 and account_age_months < 7 THEN (CASE WHEN paid_user_metrics.license_utilization <= .10 THEN 25
                                                                            WHEN paid_user_metrics.license_utilization > .10 and paid_user_metrics.license_utilization <= .50 THEN 63
                                                                            WHEN paid_user_metrics.license_utilization > .50 THEN 88
                                                                            else NULL end)
        WHEN account_age_months >= 7 and account_age_months < 10 THEN (CASE WHEN paid_user_metrics.license_utilization <= .50 THEN 25
                                                                            WHEN paid_user_metrics.license_utilization > .50 and paid_user_metrics.license_utilization <= .75 THEN 63
                                                                            WHEN paid_user_metrics.license_utilization > .75 THEN 88
                                                                            else NULL end)
        WHEN account_age_months >= 10 THEN (CASE WHEN paid_user_metrics.license_utilization <= .75 THEN 25
                                                WHEN paid_user_metrics.license_utilization > .75 THEN 88
                                                else NULL end)
        end) AS license_utilization_score,
        CASE WHEN license_utilization_score IS NULL THEN NULL
            WHEN license_utilization_score = 25 THEN 'Red'
            WHEN license_utilization_score = 63 THEN 'Yellow'
            WHEN license_utilization_score = 88 THEN 'Green' end AS license_utilization_color,

-- user engagement metrics --
        paid_user_metrics.last_activity_28_days_user, -- unique active users l28d --
        CASE WHEN paid_user_metrics.delivery_type = 'SaaS' THEN NULL
            WHEN paid_user_metrics.delivery_type = 'Self-Managed' THEN div0(paid_user_metrics.last_activity_28_days_user, paid_user_metrics.billable_user_count) end AS user_engagement,
        CASE WHEN user_engagement IS NULL THEN NULL
            WHEN user_engagement < .50 THEN 25
            WHEN user_engagement >= .50 and user_engagement < .80 THEN 63
            WHEN user_engagement >= .80 THEN 88 end AS user_engagement_score,
        CASE WHEN user_engagement_score IS NULL THEN NULL
            WHEN user_engagement_score = 25 THEN 'Red'
            WHEN user_engagement_score = 63 THEN 'Yellow'
            WHEN user_engagement_score = 88 THEN 'Green' end AS user_engagement_color,

-- scm metrics --
        CASE WHEN instance_identifier IS NULL THEN NULL else paid_user_metrics.action_monthly_active_users_project_repo_28_days_user end AS action_monthly_active_users_project_repo_28_days_user_clean, -- git operation users l28d --
        div0(action_monthly_active_users_project_repo_28_days_user_clean, paid_user_metrics.license_user_count) AS git_operation_utilization,
        CASE WHEN git_operation_utilization IS NULL THEN NULL
            WHEN git_operation_utilization < .25 THEN 25
            WHEN git_operation_utilization >= .25 and git_operation_utilization < .50 THEN 63
            WHEN git_operation_utilization >= .50 THEN 88 end AS scm_score,
        CASE WHEN scm_score IS NULL THEN NULL
            WHEN scm_score = 25 THEN 'Red'
            WHEN scm_score = 63 THEN 'Yellow'
            WHEN scm_score = 88 THEN 'Green' end AS scm_color,

-- cd metrics --
        paid_user_metrics.deployments_28_days_event,
        paid_user_metrics.successful_deployments_28_days_event,
        paid_user_metrics.failed_deployments_28_days_event,
        (paid_user_metrics.successful_deployments_28_days_event + paid_user_metrics.failed_deployments_28_days_event) AS completed_deployments_l28d,
        paid_user_metrics.releases_28_days_user,
        paid_user_metrics.projects_all_time_event,
        paid_user_metrics.environments_all_time_event,
        div0(paid_user_metrics.deployments_28_days_user, paid_user_metrics.license_user_count) AS deployments_utilization_cd,
        CASE WHEN deployments_utilization_cd IS NULL THEN NULL
            WHEN deployments_utilization_cd < .05 THEN 25
            WHEN deployments_utilization_cd >= .05 and deployments_utilization_cd <= .12 THEN 63
            WHEN deployments_utilization_cd > .12 THEN 88 end AS deployments_utilization_cd_score,
        CASE WHEN deployments_utilization_cd_score IS NULL THEN NULL
            WHEN deployments_utilization_cd_score = 25 THEN 'Red'
            WHEN deployments_utilization_cd_score = 63 THEN 'Yellow'
            WHEN deployments_utilization_cd_score = 88 THEN 'Green' end AS deployments_utilization_cd_color,
        div0(paid_user_metrics.deployments_28_days_event,paid_user_metrics.license_user_count) AS deployments_per_user_l28d_cd,
        CASE WHEN deployments_per_user_l28d_cd IS NULL THEN NULL
            WHEN deployments_per_user_l28d_cd < 2 THEN 25
            WHEN deployments_per_user_l28d_cd >= 2 and deployments_per_user_l28d_cd <= 7 THEN 63
            WHEN deployments_per_user_l28d_cd > 7 THEN 88 end AS deployments_per_user_l28d_cd_score,
        CASE WHEN deployments_per_user_l28d_cd_score IS NULL THEN NULL
            WHEN deployments_per_user_l28d_cd_score = 25 THEN 'Red'
            WHEN deployments_per_user_l28d_cd_score = 63 THEN 'Yellow'
            WHEN deployments_per_user_l28d_cd_score = 88 THEN 'Green' end AS deployments_per_user_l28d_cd_color,
        div0(successful_deployments_28_days_event, completed_deployments_l28d) AS successful_deployments_pct,
        CASE WHEN successful_deployments_pct IS NULL THEN NULL
            WHEN successful_deployments_pct < .25 THEN 25
            WHEN successful_deployments_pct >= .25 and successful_deployments_pct <= .80 THEN 63
            WHEN successful_deployments_pct > .80 THEN 88 end AS successful_deployments_pct_score,
        CASE WHEN successful_deployments_pct_score IS NULL THEN NULL
            WHEN successful_deployments_pct_score = 25 THEN 'Red'
            WHEN successful_deployments_pct_score = 63 THEN 'Yellow'
            WHEN successful_deployments_pct_score = 88 THEN 'Green' end AS successful_deployments_pct_color,
        div0(releases_28_days_user, license_user_count) AS user_releASes_utilization,
        CASE WHEN user_releASes_utilization IS NULL THEN NULL
            WHEN user_releASes_utilization = 0 THEN 25
            WHEN user_releASes_utilization > 0 and user_releASes_utilization <= .015 THEN 63
            WHEN user_releASes_utilization > .015 THEN 88 end AS user_releases_utilization_score,
        CASE WHEN user_releases_utilization_score IS NULL THEN NULL
            WHEN user_releases_utilization_score = 25 THEN 'Red'
            WHEN user_releases_utilization_score = 63 THEN 'Yellow'
            WHEN user_releases_utilization_score = 88 THEN 'Green' end AS user_releASes_utilization_color,
        (IFF(deployments_utilization_cd_score IS NOT NULL, 1, 0)
            + IFF(deployments_per_user_l28d_cd_score IS NOT NULL, 1, 0)
            + IFF(successful_deployments_pct_score IS NOT NULL, 1, 0)
            + IFF(user_releases_utilization_score IS NOT NULL, 1, 0))                  AS cd_measure_count,
        (zeroifNULL(deployments_utilization_cd_score)
            + zeroifNULL(deployments_per_user_l28d_cd_score)
            + zeroifNULL(successful_deployments_pct_score)
            + zeroifNULL(user_releases_utilization_score)) / nullif(cd_measure_count,0)          AS cd_score,
        IFF(cd_measure_count = 0, NULL, (CASE WHEN cd_score <= 50 THEN 'Red'
                                                WHEN cd_score > 50 and cd_score <= 75 THEN 'Yellow'
                                                WHEN cd_score > 75 THEN 'Green' end)) AS cd_color,

-- ci metrics --
        paid_user_metrics.deployments_28_days_user,
        paid_user_metrics.ci_pipelines_28_days_user,
        div0(paid_user_metrics.deployments_28_days_event,paid_user_metrics.license_user_count) AS deployments_per_user_l28d,
        CASE WHEN deployments_per_user_l28d IS NULL THEN NULL
            WHEN deployments_per_user_l28d < 3 THEN 25
            WHEN deployments_per_user_l28d >= 3 and deployments_per_user_l28d < 8 THEN 63
            WHEN deployments_per_user_l28d >= 8 THEN 88 end AS deployments_per_user_l28d_score,
        CASE WHEN deployments_per_user_l28d_score IS NULL THEN NULL
            WHEN deployments_per_user_l28d_score = 25 THEN 'Red'
            WHEN deployments_per_user_l28d_score = 63 THEN 'Yellow'
            WHEN deployments_per_user_l28d_score = 88 THEN 'Green' end AS deployments_per_user_l28d_color,
        div0(paid_user_metrics.deployments_28_days_user, paid_user_metrics.license_user_count) AS deployments_utilization_ci,
        CASE WHEN deployments_utilization_ci IS NULL THEN NULL
            WHEN deployments_utilization_ci < .05 THEN 25
            WHEN deployments_utilization_ci >= .05 and deployments_utilization_ci < .12 THEN 63
            WHEN deployments_utilization_ci >= .12 THEN 88 end AS deployments_utilization_score_ci,
        CASE WHEN deployments_utilization_score_ci IS NULL THEN NULL
            WHEN deployments_utilization_score_ci = 25 THEN 'Red'
            WHEN deployments_utilization_score_ci = 63 THEN 'Yellow'
            WHEN deployments_utilization_score_ci = 88 THEN 'Green' end AS deployments_utilization_color_ci,
        div0(paid_user_metrics.ci_pipelines_28_days_user, paid_user_metrics.license_user_count) AS ci_pipeline_utilization,
        CASE WHEN ci_pipeline_utilization IS NULL THEN NULL
            WHEN ci_pipeline_utilization < .25 THEN 25
            WHEN ci_pipeline_utilization >= .25 and ci_pipeline_utilization < .50 THEN 63
            WHEN ci_pipeline_utilization >= .50 THEN 88 end AS ci_pipeline_utilization_score,
        CASE WHEN ci_pipeline_utilization_score IS NULL THEN NULL
            WHEN ci_pipeline_utilization_score = 25 THEN 'Red'
            WHEN ci_pipeline_utilization_score = 63 THEN 'Yellow'
            WHEN ci_pipeline_utilization_score = 88 THEN 'Green' end               AS ci_pipeline_utilization_color,
        (IFF(deployments_per_user_l28d_score IS NOT NULL, 1, 0)
            + IFF(deployments_utilization_score_ci IS NOT NULL, 1, 0)
            + IFF(ci_pipeline_utilization_score IS NOT NULL, 1, 0))                  AS ci_meASure_count,
        (zeroifNULL(deployments_per_user_l28d_score)
            + zeroifNULL(deployments_utilization_score_ci)
            + zeroifNULL(ci_pipeline_utilization_score)) / nullif(ci_meASure_count,0)          AS ci_score,
        IFF(ci_meASure_count = 0, NULL, (CASE WHEN ci_score <= 50 THEN 'Red'
                                                WHEN ci_score > 50 and ci_score <= 75 THEN 'Yellow'
                                                WHEN ci_score > 75 THEN 'Green' end)) AS ci_color,

-- devsecops metrics --
        paid_user_metrics.user_unique_users_all_secure_scanners_28_days_user,
        paid_user_metrics.user_container_scanning_jobs_28_days_user,
        paid_user_metrics.user_secret_detection_jobs_28_days_user,
        div0(paid_user_metrics.user_unique_users_all_secure_scanners_28_days_user, paid_user_metrics.license_user_count) AS secure_scanners_utilization,
        CASE WHEN secure_scanners_utilization IS NULL THEN NULL
            WHEN secure_scanners_utilization <= .05 THEN 25
            WHEN secure_scanners_utilization > .05 and secure_scanners_utilization < .20 THEN 63
            WHEN secure_scanners_utilization >= .20 THEN 88 end                     AS secure_scanners_utilization_score,
        CASE WHEN secure_scanners_utilization_score IS NULL THEN NULL
            WHEN secure_scanners_utilization_score = 25 THEN 'Red'
            WHEN secure_scanners_utilization_score = 63 THEN 'Yellow'
            WHEN secure_scanners_utilization_score = 88 THEN 'Green' end            AS secure_scanners_utilization_color,
        div0(user_container_scanning_jobs_28_days_user, license_user_count)          AS container_scanning_jobs_utilization,
        CASE WHEN container_scanning_jobs_utilization IS NULL THEN NULL
            WHEN container_scanning_jobs_utilization <= .03 THEN 25
            WHEN container_scanning_jobs_utilization > .03 and container_scanning_jobs_utilization < .10 THEN 63
            WHEN container_scanning_jobs_utilization >= .10 THEN 88 end                     AS container_scanning_jobs_utilization_score,
        CASE WHEN container_scanning_jobs_utilization_score IS NULL THEN NULL
            WHEN container_scanning_jobs_utilization_score = 25 THEN 'Red'
            WHEN container_scanning_jobs_utilization_score = 63 THEN 'Yellow'
            WHEN container_scanning_jobs_utilization_score = 88 THEN 'Green' end            AS container_scanning_jobs_utilization_color,
        div0(user_secret_detection_jobs_28_days_user, license_user_count)            AS secret_detection_jobs_utilization,
        CASE WHEN secret_detection_jobs_utilization IS NULL THEN NULL
            WHEN secret_detection_jobs_utilization <= .06 THEN 25
            WHEN secret_detection_jobs_utilization > .06 and secret_detection_jobs_utilization < .20 THEN 63
            WHEN secret_detection_jobs_utilization >= .20 THEN 88 end                     AS secret_detection_jobs_utilization_score,
        CASE WHEN secret_detection_jobs_utilization_score IS NULL THEN NULL
            WHEN secret_detection_jobs_utilization_score = 25 THEN 'Red'
            WHEN secret_detection_jobs_utilization_score = 63 THEN 'Yellow'
            WHEN secret_detection_jobs_utilization_score = 88 THEN 'Green' end            AS secret_detection_jobs_utilization_color,
        (IFF(secure_scanners_utilization_score IS NOT NULL, 1, 0)
            + IFF(container_scanning_jobs_utilization_score IS NOT NULL, 1, 0)
            + IFF(secret_detection_jobs_utilization_score IS NOT NULL, 1, 0))                  AS devsecops_meASure_count,
        (zeroifNULL(secure_scanners_utilization_score)
            + zeroifNULL(container_scanning_jobs_utilization_score)
            + zeroifNULL(secret_detection_jobs_utilization_score)) / nullif(devsecops_meASure_count,0)          AS devsecops_score,
        IFF(devsecops_meASure_count = 0, NULL, (CASE WHEN devsecops_score <= 50 THEN 'Red'
                                                    WHEN devsecops_score > 50 and devsecops_score <= 75 THEN 'Yellow'
                                                    WHEN devsecops_score > 75 THEN 'Green' end)) AS devsecops_color,
        IFF(mart_arr.product_tier_name like '%Ultimate%', devsecops_score, NULL) AS devsecops_score_ultimate_only,
        IFF(mart_arr.product_tier_name like '%Ultimate%', devsecops_color, NULL) AS devsecops_color_ultimate_only,

-- overall product score --
        IFF(license_utilization_score IS NULL, 0, .30) AS license_utilization_weight,
        IFF(user_engagement_score IS NULL, 0, .10) AS user_engagement_weight,
        IFF(scm_score IS NULL, 0, .15) AS scm_weight,
        IFF(cd_score IS NULL, 0, .15) AS cd_weight,
        IFF(ci_score IS NULL, 0, .15) AS ci_weight,
        IFF(devsecops_score_ultimate_only IS NULL, 0, .15) AS devsecops_weight,
        license_utilization_weight + user_engagement_weight + scm_weight + cd_weight + ci_weight + devsecops_weight AS remaining_weight,
        license_utilization_weight/ nullif(remaining_weight,0) AS adjusted_license_utilization_weight,
        user_engagement_weight/nullif(remaining_weight,0) AS adjusted_user_engagement_weight,
        scm_weight/nullif(remaining_weight,0) AS adjusted_scm_weight,
        cd_weight/nullif(remaining_weight,0) AS adjusted_cd_weight,
        ci_weight/nullif(remaining_weight,0) AS adjusted_ci_weight,
        devsecops_weight/nullif(remaining_weight,0) AS adjusted_devsecops_weight,
        zeroifNULL(license_utilization_score) * adjusted_license_utilization_weight AS adjusted_license_utilization_score,
        zeroifNULL(user_engagement_score) * adjusted_user_engagement_weight AS adjusted_user_engagement_score,
        zeroifNULL(scm_score) * adjusted_scm_weight AS adjusted_scm_score,
        zeroifNULL(cd_score) * adjusted_cd_weight AS adjusted_cd_score,
        zeroifNULL(ci_score) * adjusted_ci_weight AS adjusted_ci_score,
        zeroifNULL(devsecops_score_ultimate_only) * adjusted_devsecops_weight AS adjusted_devsecops_score,
        adjusted_license_utilization_score + adjusted_user_engagement_score + adjusted_scm_score + adjusted_cd_score + adjusted_ci_score + adjusted_devsecops_score AS overall_product_score,
        CASE WHEN overall_product_score <= 50 THEN 'Red'
            WHEN overall_product_score > 50 and overall_product_score <= 75 THEN 'Yellow'
            WHEN overall_product_score > 75 THEN 'Green' end AS overall_product_color

FROM paid_user_metrics
LEFT JOIN dim_crm_account
    ON paid_user_metrics.dim_crm_account_id = dim_crm_account.dim_crm_account_id
LEFT JOIN mart_arr
    ON paid_user_metrics.dim_subscription_id_original = mart_arr.dim_subscription_id_original
    AND paid_user_metrics.snapshot_month = mart_arr.arr_month
WHERE paid_user_metrics.license_user_count != 0
qualify row_number() OVER (PARTITION BY paid_user_metrics.snapshot_month, instance_identifier ORDER BY paid_user_metrics.ping_created_at DESC NULLs last) = 1

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@jngCES",
    updated_by="@jngCES",
    created_date="2023-03-30",
    updated_date="2023-04-05"
) }}