WITH date_spine AS (

  SELECT date_day FROM {{ ref('dim_date') }}
  WHERE date_day < GETDATE() AND date_day >= '2020-01-01'
),

infralabel_pl AS (

  SELECT
    date_spine.date_day,
    NULL                      AS gcp_project_id,
    NULL                      AS gcp_service_description,
    NULL                      AS gcp_sku_description,
    infralabel_pl.infra_label,
    NULL                      AS env_label,
    NULL                      AS runner_label,
    LOWER(infralabel_pl.type) AS pl_category,
    infralabel_pl.allocation  AS pl_percent,
    'infralabel_pl'           AS from_mapping
  FROM date_spine
  CROSS JOIN {{ ref('infralabel_pl') }}

),

projects_pl AS (

  SELECT
    date_spine.date_day,
    projects_pl.project_id  AS gcp_project_id,
    NULL                    AS gcp_service_description,
    NULL                    AS gcp_sku_description,
    NULL                    AS infra_label,
    NULL                    AS env_label,
    NULL                    AS runner_label,
    LOWER(projects_pl.type) AS pl_category,
    projects_pl.allocation  AS pl_percent,
    'projects_pl'           AS from_mapping
  FROM date_spine
  CROSS JOIN {{ ref ('projects_pl') }}

),

repo_storage_pl_daily AS (

  WITH sku_list AS (SELECT 'SSD backed PD Capacity' AS sku
    UNION ALL
    SELECT 'Balanced PD Capacity'
    UNION ALL
    SELECT 'Storage PD Snapshot in US'
    UNION ALL
    SELECT 'Storage PD Capacity'
  )

  SELECT
    snapshot_day                               AS date_day,
    'gitlab-production'                        AS gcp_project_id,
    'Compute Engine'                           AS gcp_service_description,
    sku_list.sku                               AS gcp_sku_description,
    'gitaly'                                   AS infra_label,
    NULL                                       AS env_label,
    NULL                                       AS runner_label,
    LOWER(repo_storage_pl_daily.finance_pl)    AS pl_category,
    repo_storage_pl_daily.percent_repo_size_gb AS pl_percent,
    'repo_storage_pl_daily'                    AS from_mapping
  FROM {{ ref ('repo_storage_pl_daily') }}
  CROSS JOIN sku_list
),

repo_storage_pl_daily_ext AS (

  SELECT
    snapshot_day                               AS date_day,
    'gitlab-production'                        AS gcp_project_id,
    NULL                                       AS gcp_service_description,
    NULL                                       AS gcp_sku_description,
    'gitaly'                                   AS infra_label,
    NULL                                       AS env_label,
    NULL                                       AS runner_label,
    LOWER(repo_storage_pl_daily.finance_pl)    AS pl_category,
    repo_storage_pl_daily.percent_repo_size_gb AS pl_percent,
    'repo_storage_pl_daily'                    AS from_mapping
  FROM {{ ref ('repo_storage_pl_daily') }}

),

sandbox_projects_pl AS (

  SELECT
    date_spine.date_day,
    sandbox_projects_pl.gcp_project_id        AS gcp_project_id,
    NULL                                      AS gcp_service_description,
    NULL                                      AS gcp_sku_description,
    NULL                                      AS infra_label,
    NULL                                      AS env_label,
    NULL                                      AS runner_label,
    LOWER(sandbox_projects_pl.classification) AS pl_category,
    1                                         AS pl_percent,
    'sandbox_projects_pl'                     AS from_mapping
  FROM date_spine
  CROSS JOIN {{ ref ('sandbox_projects_pl') }}
),

container_registry_pl_daily AS (

  SELECT
    snapshot_day                                                AS date_day,
    'gitlab-production'                                         AS gcp_project_id,
    'Cloud Storage'                                             AS gcp_service_description,
    'Standard Storage US Multi-region'                          AS gcp_sku_description,
    'registry'                                                  AS infra_label,
    NULL                                                        AS env_label,
    NULL                                                        AS runner_label,
    LOWER(container_registry_pl_daily.finance_pl)               AS pl_category,
    container_registry_pl_daily.percent_container_registry_size AS pl_percent,
    'container_registry_pl_daily'                               AS from_mapping
  FROM {{ ref ('container_registry_pl_daily') }}
  WHERE snapshot_day > '2022-06-10'

),

container_registry_pl_daily_ext AS (

  SELECT
    snapshot_day                                                AS date_day,
    'gitlab-production'                                         AS gcp_project_id,
    NULL                                                        AS gcp_service_description,
    NULL                                                        AS gcp_sku_description,
    'registry'                                                  AS infra_label,
    NULL                                                        AS env_label,
    NULL                                                        AS runner_label,
    LOWER(container_registry_pl_daily.finance_pl)               AS pl_category,
    container_registry_pl_daily.percent_container_registry_size AS pl_percent,
    'container_registry_pl_daily'                               AS from_mapping
  FROM {{ ref ('container_registry_pl_daily') }}
  WHERE snapshot_day > '2022-06-10'

),

build_artifacts_pl_daily AS (

  SELECT
    snapshot_day                                          AS date_day,
    'gitlab-production'                                   AS gcp_project_id,
    'Cloud Storage'                                       AS gcp_service_description,
    'Standard Storage US Multi-region'                    AS gcp_sku_description,
    'build_artifacts'                                     AS infra_label,
    NULL                                                  AS env_label,
    NULL                                                  AS runner_label,
    LOWER(build_artifacts_pl_daily.finance_pl)            AS pl_category,
    build_artifacts_pl_daily.percent_build_artifacts_size AS pl_percent,
    'build_artifacts_pl_daily'                            AS from_mapping
  FROM {{ ref ('build_artifacts_pl_daily') }}

),

build_artifacts_pl_dev_daily AS (

  SELECT DISTINCT
    snapshot_day                       AS date_day,
    'gitlab-production'                AS gcp_project_id,
    'Cloud Storage'                    AS gcp_service_description,
    'Standard Storage US Multi-region' AS gcp_sku_description,
    'build_artifacts'                  AS infra_label,
    'dev'                              AS env_label,
    NULL                               AS runner_label,
    'internal'                         AS pl_category,
    1                                  AS pl_percent,
    'build_artifacts_pl_dev_daily'     AS from_mapping
  FROM {{ ref ('build_artifacts_pl_daily') }}

),

single_sku_pl AS (

  SELECT
    date_spine.date_day,
    NULL                              AS gcp_project_id,
    single_sku_pl.service_description AS gcp_service_description,
    single_sku_pl.sku_description     AS gcp_sku_description,
    NULL                              AS infra_label,
    NULL                              AS env_label,
    NULL                              AS runner_label,
    LOWER(single_sku_pl.type)         AS pl_category,
    single_sku_pl.allocation          AS pl_percent,
    'single_sku_pl'                   AS from_mapping
  FROM date_spine
  CROSS JOIN {{ ref ('single_sku_pl') }}

),

runner_shared_gitlab_org AS (
  -- shared gitlab org runner
  SELECT DISTINCT
    reporting_day                      AS date_day,
    'gitlab-ci-155816'                 AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    '1 - shared gitlab org runners'    AS runner_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 1'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '1 - shared gitlab org runners'

),

runner_saas_small AS (
  -- small saas runners with small infra label
  SELECT DISTINCT
    reporting_day                      AS date_day,
    NULL                               AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    '2 - shared saas runners - small'  AS runner_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 2'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '2 - shared saas runners - small'

),

runner_saas_small_ext AS (
  -- extension: applying same split to remaining resources in gitlab-ci-plan-free-* projects
  SELECT DISTINCT
    reporting_day                      AS date_day,
    'gitlab-ci-plan-free-%'            AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    NULL                               AS runner_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 2'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '2 - shared saas runners - small'

),

runner_saas_medium AS (

  SELECT DISTINCT
    reporting_day                      AS date_day,
    NULL                               AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    '3 - shared saas runners - medium' AS runner_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 3'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '3 - shared saas runners - medium'

),

runner_saas_medium_ext AS (

  SELECT DISTINCT
    reporting_day                      AS date_day,
    'gitlab-r-saas-l-m-%'              AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    NULL                               AS runner_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 3'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '3 - shared saas runners - medium'

),

runner_saas_large AS (

  SELECT DISTINCT
    reporting_day                      AS date_day,
    NULL                               AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    '4 - shared saas runners - large'  AS runner_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 4'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '4 - shared saas runners - large'

),

runner_saas_large_ext AS (

  SELECT DISTINCT
    reporting_day                      AS date_day,
    'gitlab-r-saas-l-l-%'              AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    NULL                               AS runner_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 4'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '4 - shared saas runners - large'

),

haproxy_pl AS (

  SELECT * FROM {{ ref('haproxy_backend_pl') }}

),

haproxy_usage AS (

  SELECT * FROM {{ ref('haproxy_backend_ratio_daily') }}

),

haproxy_isp AS (

  SELECT
    haproxy_usage.date_day                                        AS date_day,
    'gitlab-production'                                           AS gcp_project_id,
    'Compute Engine'                                              AS gcp_service_description,
    'Network Egress via Carrier Peering Network - Americas Based' AS gcp_sku_description,
    'shared'                                                      AS infra_label,
    NULL                                                          AS env_label,
    NULL                                                          AS runner_label,
    haproxy_pl.type                                               AS pl_category,
    haproxy_usage.percent_backend_ratio * haproxy_pl.allocation   AS pl_percent,
    CONCAT('haproxy-', haproxy_usage.backend_category)            AS from_mapping
  FROM haproxy_usage
  INNER JOIN haproxy_pl
    ON haproxy_usage.backend_category = haproxy_pl.metric_backend

),

haproxy_inter AS (


  SELECT
    haproxy_usage.date_day                                      AS date_day,
    'gitlab-production'                                         AS gcp_project_id,
    'Compute Engine'                                            AS gcp_service_description,
    'Network Inter Zone Egress'                                 AS gcp_sku_description,
    'shared'                                                    AS infra_label,
    NULL                                                        AS env_label,
    NULL                                                        AS runner_label,
    haproxy_pl.type                                             AS pl_category,
    haproxy_usage.percent_backend_ratio * haproxy_pl.allocation AS pl_percent,
    CONCAT('haproxy-', haproxy_usage.backend_category)          AS from_mapping
  FROM haproxy_usage
  INNER JOIN haproxy_pl
    ON haproxy_usage.backend_category = haproxy_pl.metric_backend

),

cte_append AS (SELECT *
  FROM infralabel_pl
  UNION ALL
  SELECT *
  FROM projects_pl
  UNION ALL
  SELECT *
  FROM repo_storage_pl_daily
  UNION ALL
  SELECT *
  FROM repo_storage_pl_daily_ext
  UNION ALL
  SELECT *
  FROM sandbox_projects_pl
  UNION ALL
  SELECT *
  FROM container_registry_pl_daily
  UNION ALL
  SELECT *
  FROM container_registry_pl_daily_ext
  UNION ALL
  SELECT *
  FROM build_artifacts_pl_daily
  UNION ALL
  SELECT *
  FROM build_artifacts_pl_dev_daily
  UNION ALL
  SELECT *
  FROM single_sku_pl
  UNION ALL
  SELECT *
  FROM runner_shared_gitlab_org
  UNION ALL
  SELECT *
  FROM runner_saas_small
  UNION ALL
  SELECT *
  FROM runner_saas_small_ext
  UNION ALL
  SELECT *
  FROM runner_saas_medium
  UNION ALL
  SELECT *
  FROM runner_saas_medium_ext
  UNION ALL
  SELECT *
  FROM runner_saas_large
  UNION ALL
  SELECT *
  FROM runner_saas_large_ext
  UNION ALL
  SELECT *
  FROM haproxy_isp
  UNION ALL
  SELECT *
  FROM haproxy_inter
)

SELECT
  date_day,
  gcp_project_id,
  gcp_service_description,
  gcp_sku_description,
  infra_label,
  env_label,
  runner_label,
  lower(pl_category)      AS pl_category,
  pl_percent,
  LISTAGG(DISTINCT from_mapping, ' || ') WITHIN GROUP (
    ORDER BY from_mapping ASC) AS from_mapping
FROM cte_append
{{ dbt_utils.group_by(n=9) }}
