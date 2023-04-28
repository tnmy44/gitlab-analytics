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
      WHEN LOWER(ci_runner_description) LIKE 'green-_.saas-linux-medium-amd64%' THEN 'green-_.saas-linux-medium-amd64'
      WHEN LOWER(ci_runner_description) LIKE 'green-_.saas-linux-large-amd64%' THEN 'green-_.saas-linux-large-amd64'
      WHEN LOWER(ci_runner_description) LIKE 'blue-_.saas-linux-large-amd64%' THEN 'blue-_.saas-linux-large-amd64'
      WHEN LOWER(ci_runner_description) LIKE 'blue-_.saas-linux-medium-amd64%' THEN 'blue-_.saas-linux-medium-amd64'
      WHEN LOWER(ci_runner_description) LIKE 'macos shared%' THEN 'macos shared runners'
      ELSE ci_runner_manager
    END                                          AS ci_runner_manager,

    is_paid_by_gitlab,

    CASE
      WHEN LOWER(ci_runner_description) LIKE 'green-%.saas-linux-large-amd64-gpu.%' THEN 'green-_.saas-linux-large-amd64-gpu.'
      WHEN LOWER(ci_runner_description) LIKE 'green-%.saas-linux-large-amd64.%' THEN 'green-_.saas-linux-large-amd64.'
      WHEN LOWER(ci_runner_description) LIKE 'green-%.saas-linux-medium-amd64.%' THEN 'green-_.saas-linux-medium-amd64.'

      WHEN LOWER(ci_runner_description) LIKE 'blue-%.saas-linux-large-amd64.%' THEN 'blue-_.saas-linux-large-amd64.'
      WHEN LOWER(ci_runner_description) LIKE 'blue-%.saas-linux-medium-amd64.%' THEN 'blue-_.saas-linux-medium-amd64.'

      WHEN LOWER(ci_runner_description) LIKE '_-blue.shared-gitlab-org%' THEN '_-blue.shared-gitlab-org'
      WHEN LOWER(ci_runner_description) LIKE '_-green.shared-gitlab-org%' THEN '_-green.shared-gitlab-org'

      WHEN LOWER(ci_runner_description) LIKE 'green-_.private.runners-manager%' THEN 'green-_.private.runners-manager'
      WHEN LOWER(ci_runner_description) LIKE 'blue-_.private.runners-manager%' THEN 'blue-_.private.runners-manager'

      WHEN LOWER(ci_runner_description) LIKE '_-blue.shared.runners-manager%' THEN '_-blue.shared.runners-manager'
      WHEN LOWER(ci_runner_description) LIKE '_-green.shared.runners-manager%' THEN '_-green.shared.runners-manager'

      WHEN LOWER(ci_runner_description) LIKE 'windows-shared-runners-manager-%' THEN 'windows-shared-runners-manager-'


    END                                          AS ci_runner_description,


    CASE
      WHEN LOWER(ci_runner_description) LIKE '%saas-linux-large%' THEN 'SaaS Runners Linux - Large'
      WHEN LOWER(ci_runner_description) LIKE '%saas-linux-medium%' THEN 'SaaS Runners - Medium'
      WHEN LOWER(ci_runner_description) LIKE '%.shared.runners-manager.%' THEN 'SaaS Runners Linux - Small'
      WHEN LOWER(ci_runner_description) LIKE 'shared-runners-manager-%' THEN 'SaaS Runners Linux - Small'
      WHEN LOWER(ci_runner_description) LIKE 'windows-shared-runners-manager-%' THEN 'SaaS Runners Windows - Medium'
      WHEN LOWER(ci_runner_description) LIKE 'macos shared%' THEN ci_runner_description
      ELSE 'Other'
    END                                          AS dashboard_mapping,

    SUM(ci_build_duration_in_s) / 60             AS ci_build_minutes

  FROM {{ ref('fct_ci_runner_activity') }} --common.fct_ci_runner_activity 
    JOIN {{ ref('dim_ci_runner') }} --common.dim_ci_runner
    ON fct_ci_runner_activity.dim_ci_runner_id = dim_ci_runner.dim_ci_runner_id
    JOIN {{ ref('dim_namespace') }} --common.dim_namespace
    ON fct_ci_runner_activity.dim_namespace_id = dim_namespace.dim_namespace_id
    JOIN {{ ref('prep_gitlab_dotcom_plan') }} --common_prep.prep_gitlab_dotcom_plan
    ON fct_ci_runner_activity.dim_plan_id = prep_gitlab_dotcom_plan.dim_plan_id
  WHERE DATE_TRUNC('month', ci_build_started_at) >= '2023-02-01'
    AND ci_build_finished_at IS NOT NULL
    AND namespace_creator_is_blocked = FALSE
  GROUP BY 1, 2, 3, 4, 5, 6, 7

)

SELECT
  reporting_day,

  CASE
    WHEN runner_type = 'Self-Managed Runners' AND ci_runner_manager = 'private-runner-mgr' THEN '6 - private internal runners'
    WHEN runner_type = 'Self-Managed Runners' AND ci_runner_manager = 'private-runner-mgr' THEN '6 - private internal runners'
    WHEN runner_type = 'Self-Managed Runners' AND is_paid_by_gitlab = TRUE THEN '6 - private internal runners'

    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'shared-gitlab-org-runner-mgr' THEN '1 - shared gitlab org runners'
    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'shared-gitlab-org-runner-mgr' THEN '1 - shared gitlab org runners'

    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'linux-runner-mgr' THEN '2 - shared saas runners - small'
    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'linux-runner-mgr' THEN '2 - shared saas runners - small'

    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'green-_.saas-linux-medium-amd64' THEN '3 - shared saas runners - medium'
    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'blue-_.saas-linux-medium-amd64' THEN '3 - shared saas runners - medium'

    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'green-_.saas-linux-large-amd64' THEN '4 - shared saas runners - large'
    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'blue-_.saas-linux-large-amd64' THEN '4 - shared saas runners - large'

    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'macos shared runners' THEN '5 - shared saas macos runners'

    WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'windows-runner-mgr' THEN '7 - shared saas windows runners'

  END                                                                                   AS mapping,
  pl,
  TRUNC(SUM(ci_build_minutes))                                                          AS total_ci_minutes,
  TRUNC(RATIO_TO_REPORT(total_ci_minutes) OVER(PARTITION BY reporting_day, mapping), 2) AS pct_ci_minutes
FROM ci_minutes
WHERE mapping IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, mapping DESC
