WITH ci_minutes AS (

  SELECT
    DATE_TRUNC('day', ci_build_started_at)::DATE AS reporting_day,

    CASE
      WHEN plan_name_modified LIKE '%trial%' THEN 'free'
      WHEN (plan_name_modified = 'ultimate' AND dim_namespace.namespace_is_internal = FALSE) THEN 'paid'
      WHEN (plan_name_modified = 'ultimate' AND dim_namespace.namespace_is_internal = TRUE) THEN 'internal'
      WHEN plan_name_modified = 'premium' THEN 'paid'
      WHEN plan_name_modified = 'default' THEN 'free'

      WHEN plan_title = 'Bronze' THEN 'paid'
      WHEN plan_title = 'Open Source Program' THEN 'free'
      WHEN plan_title = 'Free' THEN 'free'

      ELSE plan_title

    END                                          AS pl,


    CASE
      WHEN ci_runner_type_summary = 'shared' THEN 'Shared Runners'
      ELSE 'Self-Managed Runners'
    END                                          AS runner_type,

    CASE
      WHEN LOWER(ci_runner_description) LIKE '%-_.saas-linux-medium-amd64-gpu%' THEN '%-_.saas-linux-medium-amd64-gpu'
      WHEN LOWER(ci_runner_description) LIKE '%-_.saas-linux-large-amd64-gpu%' THEN '%-_.saas-linux-large-amd64-gpu'
      WHEN LOWER(ci_runner_description) LIKE '%-_.saas-linux-medium-amd64%' THEN '%-_.saas-linux-medium-amd64'
      WHEN LOWER(ci_runner_description) LIKE '%-_.saas-linux-large-amd64%' THEN '%-_.saas-linux-large-amd64'
      WHEN LOWER(ci_runner_description) LIKE '%.saas-linux-small-amd64%' THEN '%.saas-linux-small-amd64'
      WHEN LOWER(ci_runner_description) LIKE '%.saas-linux-xlarge-amd64%' THEN '%.saas-linux-xlarge-amd64'
      WHEN LOWER(ci_runner_description) LIKE 'macos shared%' THEN 'macos shared runners'
      ELSE ci_runner_manager
    END                                          AS ci_runner_manager,

    is_paid_by_gitlab,

    SUM(ci_build_duration_in_s) / 60             AS ci_build_minutes

  FROM {{ ref('fct_ci_runner_activity') }} --common.fct_ci_runner_activity 
    JOIN {{ ref('dim_ci_runner') }} --common.dim_ci_runner
    ON fct_ci_runner_activity.dim_ci_runner_id = dim_ci_runner.dim_ci_runner_id
    JOIN {{ ref('dim_namespace') }} --common.dim_namespace
    ON fct_ci_runner_activity.dim_namespace_id = dim_namespace.dim_namespace_id
    JOIN {{ ref('prep_gitlab_dotcom_plan') }} --common_prep.prep_gitlab_dotcom_plan
    ON fct_ci_runner_activity.dim_plan_id = prep_gitlab_dotcom_plan.dim_plan_id
  WHERE DATE_TRUNC('month', ci_build_started_at) >= '2023-01-01'
    AND ci_build_finished_at IS NOT NULL
    AND namespace_creator_is_blocked = FALSE
{{ dbt_utils.group_by(n=5) }}

)

SELECT
  reporting_day,

  CASE
    WHEN runner_type = 'Self-Managed Runners' AND ci_runner_manager = 'private-runner-mgr' THEN '6 - private internal runners'
    WHEN runner_type = 'Self-Managed Runners' AND is_paid_by_gitlab = TRUE THEN '6 - private internal runners'

    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'shared-gitlab-org-runner-mgr' THEN '1 - shared gitlab org runners'

    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = '%.saas-linux-small-amd64' THEN '2 - shared saas runners - small'

    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = '%-_.saas-linux-medium-amd64' AND ci_runner_manager NOT LIKE '%gpu%' THEN '3 - shared saas runners - medium'

    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = '%-_.saas-linux-large-amd64' AND ci_runner_manager NOT LIKE '%gpu%' THEN '4 - shared saas runners - large'

    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = '%.saas-linux-xlarge-amd64' THEN '10 - shared saas runners - xlarge'

    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'macos shared runners' THEN '5 - shared saas macos runners'

    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'windows-runner-mgr' THEN '7 - shared saas windows runners'
    
    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = '%-_.saas-linux-medium-amd64-gpu' THEN '8 - shared saas runners gpu - medium'

    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = '%-_.saas-linux-large-amd64-gpu' THEN '9 - shared saas runners gpu - large'

  END                                                                                   AS mapping,
  pl,
  TRUNC(SUM(ci_build_minutes))                                                          AS total_ci_minutes,
  TRUNC(RATIO_TO_REPORT(total_ci_minutes) OVER(PARTITION BY reporting_day, mapping), 2) AS pct_ci_minutes
FROM ci_minutes
WHERE mapping IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, mapping DESC
