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
    NULL                      AS folder_label,
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
    NULL                    AS folder_label,
    LOWER(projects_pl.type) AS pl_category,
    projects_pl.allocation  AS pl_percent,
    'projects_pl'           AS from_mapping
  FROM date_spine
  CROSS JOIN {{ ref ('projects_pl') }}

),

folder_pl AS (

  SELECT
    date_spine.date_day,
    NULL                  AS gcp_project_id,
    NULL                  AS gcp_service_description,
    NULL                  AS gcp_sku_description,
    NULL                  AS infra_label,
    NULL                  AS env_label,
    NULL                  AS runner_label,
    folder_id             AS folder_label,
    LOWER(folder_pl.type) AS pl_category,
    folder_pl.allocation  AS pl_percent,
    'folder_pl'           AS from_mapping
  FROM date_spine
  CROSS JOIN {{ ref ('folder_pl') }}
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
    NULL                                       AS folder_label,
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
    NULL                                       AS folder_label,
    LOWER(repo_storage_pl_daily.finance_pl)    AS pl_category,
    repo_storage_pl_daily.percent_repo_size_gb AS pl_percent,
    'repo_storage_pl_daily'                    AS from_mapping
  FROM {{ ref ('repo_storage_pl_daily') }}

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
    NULL                                                        AS folder_label,
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
    NULL                                                        AS folder_label,
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
    NULL                                                  AS gcp_sku_description,
    'build_artifacts'                                     AS infra_label,
    NULL                                                  AS env_label,
    NULL                                                  AS runner_label,
    NULL                                                  AS folder_label,
    LOWER(build_artifacts_pl_daily.finance_pl)            AS pl_category,
    build_artifacts_pl_daily.percent_build_artifacts_size AS pl_percent,
    'build_artifacts_pl_daily'                            AS from_mapping
  FROM {{ ref ('build_artifacts_pl_daily') }}

),

build_artifacts_pl_dev_daily AS (

  SELECT DISTINCT
    snapshot_day                   AS date_day,
    'gitlab-production'            AS gcp_project_id,
    'Cloud Storage'                AS gcp_service_description,
    NULL                           AS gcp_sku_description,
    'build_artifacts'              AS infra_label,
    'dev'                          AS env_label,
    NULL                           AS runner_label,
    NULL                           AS folder_label,
    'internal'                     AS pl_category,
    1                              AS pl_percent,
    'build_artifacts_pl_dev_daily' AS from_mapping
  FROM {{ ref ('build_artifacts_pl_daily') }}

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
    NULL                               AS folder_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 1'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '1 - shared gitlab org runners'

),

runner_saas_small AS (
  -- small saas runners with small infra label
  SELECT
    reporting_day                      AS date_day,
    NULL                               AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    '2 - shared saas runners - small'  AS runner_label,
    NULL                               AS folder_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 2'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '2 - shared saas runners - small'

),

runner_saas_small_ext AS (
  -- extension: applying same split to remaining resources in gitlab-ci-plan-free-* projects
  WITH small_projects AS (
    SELECT 'gitlab-ci-plan-free-%' AS gcp_project_id
    UNION ALL
    SELECT 'gitlab-r-saas-l-s-amd64-%'
  )

  SELECT DISTINCT
    reporting_day                      AS date_day,
    small_projects.gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    NULL                               AS runner_label,
    NULL                               AS folder_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 2'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  CROSS JOIN small_projects
  WHERE mapping = '2 - shared saas runners - small'

),

runner_saas_medium AS (

  SELECT
    reporting_day                      AS date_day,
    NULL                               AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    '3 - shared saas runners - medium' AS runner_label,
    NULL                               AS folder_label,
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
    NULL                               AS folder_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 3'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '3 - shared saas runners - medium'

),

runner_saas_large AS (

  SELECT
    reporting_day                      AS date_day,
    NULL                               AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    '4 - shared saas runners - large'  AS runner_label,
    NULL                               AS folder_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 4'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '4 - shared saas runners - large'

),

runner_saas_large_ext AS (

  SELECT DISTINCT
    reporting_day                      AS date_day,
    'gitlab-r-saas-l-l-amd64-_'        AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    NULL                               AS runner_label,
    NULL                               AS folder_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 4'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '4 - shared saas runners - large'

),

runner_saas_xlarge AS (

  SELECT
    reporting_day                       AS date_day,
    NULL                                AS gcp_project_id,
    NULL                                AS gcp_service_description,
    NULL                                AS gcp_sku_description,
    NULL                                AS infra_label,
    NULL                                AS env_label,
    '10 - shared saas runners - xlarge' AS runner_label,
    NULL                                AS folder_label,
    ci_runners_pl_daily.pl              AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes  AS pl_percent,
    'ci_runner_pl_daily - 10'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '10 - shared saas runners - xlarge'

),

runner_saas_xlarge_ext AS (

  SELECT DISTINCT
    reporting_day                      AS date_day,
    'gitlab-r-saas-l-xl-amd64-_'       AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    NULL                               AS runner_label,
    NULL                               AS folder_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 10'          AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '10 - shared saas runners - xlarge'

),

runner_saas_medium_gpu AS (

  SELECT
    reporting_day                          AS date_day,
    NULL                                   AS gcp_project_id,
    NULL                                   AS gcp_service_description,
    NULL                                   AS gcp_sku_description,
    NULL                                   AS infra_label,
    NULL                                   AS env_label,
    '8 - shared saas runners gpu - medium' AS runner_label,
    NULL                                   AS folder_label,
    ci_runners_pl_daily.pl                 AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes     AS pl_percent,
    'ci_runner_pl_daily - 8'               AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '8 - shared saas runners gpu - medium'

),

runner_saas_medium_ext_gpu AS (

  SELECT DISTINCT
    reporting_day                      AS date_day,
    '%-r-saas-l-m-%gpu%'               AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    NULL                               AS runner_label,
    NULL                               AS folder_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 8'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '8 - shared saas runners gpu - medium'

),

runner_saas_large_gpu AS (

  SELECT
    reporting_day                         AS date_day,
    NULL                                  AS gcp_project_id,
    NULL                                  AS gcp_service_description,
    NULL                                  AS gcp_sku_description,
    NULL                                  AS infra_label,
    NULL                                  AS env_label,
    '9 - shared saas runners gpu - large' AS runner_label,
    NULL                                  AS folder_label,
    ci_runners_pl_daily.pl                AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes    AS pl_percent,
    'ci_runner_pl_daily - 9'              AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '8 - shared saas runners gpu - medium' --to apply historic medium pl to large
    AND reporting_day <= '2023-06-22'

  UNION ALL

  SELECT
    reporting_day                         AS date_day,
    NULL                                  AS gcp_project_id,
    NULL                                  AS gcp_service_description,
    NULL                                  AS gcp_sku_description,
    NULL                                  AS infra_label,
    NULL                                  AS env_label,
    '9 - shared saas runners gpu - large' AS runner_label,
    NULL                                  AS folder_label,
    ci_runners_pl_daily.pl                AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes    AS pl_percent,
    'ci_runner_pl_daily - 9'              AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '9 - shared saas runners gpu - large'

),

runner_saas_large_ext_gpu AS (

  SELECT DISTINCT
    reporting_day                      AS date_day,
    '%-r-saas-l-l-%gpu%'               AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    NULL                               AS runner_label,
    NULL                               AS folder_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 9'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '8 - shared saas runners gpu - medium' --to apply historic medium pl to large
    AND reporting_day <= '2023-06-22'

  UNION ALL

  SELECT DISTINCT
    reporting_day                      AS date_day,
    '%-r-saas-l-l-%gpu%'               AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    NULL                               AS runner_label,
    NULL                               AS folder_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 9'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '9 - shared saas runners gpu - large'

),

runner_saas_macos AS (

  SELECT
    reporting_day                      AS date_day,
    NULL                               AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    '5 - shared saas macos runners'    AS runner_label,
    NULL                               AS folder_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 5'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '5 - shared saas macos runners'

),

runner_saas_private AS (

  SELECT
    reporting_day                      AS date_day,
    NULL                               AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    '6 - private internal runners'     AS runner_label,
    NULL                               AS folder_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 6'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '6 - private internal runners'

),

runner_saas_private_ext AS (

  SELECT DISTINCT
    reporting_day                      AS date_day,
    'gitlab-ci-private-_'              AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                               AS env_label,
    NULL                               AS runner_label,
    NULL                               AS folder_label,
    ci_runners_pl_daily.pl             AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 6'           AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  WHERE mapping = '6 - private internal runners'

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
    'Network Egress via Carrier Peering Network - Americas Based' AS gcp_sku_description,  -- specific SKU mapping
    'shared'                                                      AS infra_label,
    NULL                                                          AS env_label,
    NULL                                                          AS runner_label,
    NULL                                                          AS folder_label,
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
    'Network Inter Zone Egress'                                 AS gcp_sku_description, -- specific SKU mapping
    'shared'                                                    AS infra_label,
    NULL                                                        AS env_label,
    NULL                                                        AS runner_label,
    NULL                                                        AS folder_label,
    haproxy_pl.type                                             AS pl_category,
    haproxy_usage.percent_backend_ratio * haproxy_pl.allocation AS pl_percent,
    CONCAT('haproxy-', haproxy_usage.backend_category)          AS from_mapping
  FROM haproxy_usage
  INNER JOIN haproxy_pl
    ON haproxy_usage.backend_category = haproxy_pl.metric_backend

),

haproxy_cdn AS (
  -- rationale: apply same split on CDN thana lready exisitng haproxy split
  WITH sku_list AS (SELECT 'Networking Cloud CDN Traffic Cache Egress to North America' AS sku
    UNION ALL
    SELECT 'Networking Cloud CDN Traffic Cache Egress to Europe'
    UNION ALL
    SELECT 'Cloud CDN Cache Fill from North America to Europe'
    UNION ALL
    SELECT 'Networking Cloud CDN Traffic Cache Egress to Asia'
    UNION ALL
    SELECT 'Networking Cloud CDN Traffic Cache Egress to Oceania'
    UNION ALL
    SELECT 'Networking Cloud Nat Data Processing'
    UNION ALL
    SELECT 'Networking Cloud CDN Traffic Cache Egress to Latin America'
    UNION ALL
    SELECT 'Cloud CDN Cache Fill from North America to Asia Pacific'

  )

  SELECT
    haproxy_usage.date_day                                      AS date_day,
    'gitlab-production'                                         AS gcp_project_id,
    NULL                                                        AS gcp_service_description,
    sku_list.sku                                                AS gcp_sku_description, -- all CDN skus
    NULL                                                        AS infra_label,
    NULL                                                        AS env_label,
    NULL                                                        AS runner_label,
    NULL                                                        AS folder_label,
    haproxy_pl.type                                             AS pl_category,
    haproxy_usage.percent_backend_ratio * haproxy_pl.allocation AS pl_percent,
    CONCAT('haproxy-', haproxy_usage.backend_category)          AS from_mapping
  FROM haproxy_usage
  INNER JOIN haproxy_pl
    ON haproxy_usage.backend_category = haproxy_pl.metric_backend
  CROSS JOIN sku_list

),

cte_append AS (SELECT *
  FROM infralabel_pl
  UNION ALL
  SELECT *
  FROM projects_pl
  UNION ALL
  SELECT *
  FROM folder_pl
  UNION ALL
  SELECT *
  FROM repo_storage_pl_daily
  UNION ALL
  SELECT *
  FROM repo_storage_pl_daily_ext
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
  FROM runner_saas_xlarge
  UNION ALL
  SELECT *
  FROM runner_saas_xlarge_ext
  UNION ALL
  SELECT *
  FROM runner_saas_medium_gpu
  UNION ALL
  SELECT *
  FROM runner_saas_medium_ext_gpu
  UNION ALL
  SELECT *
  FROM runner_saas_large_gpu
  UNION ALL
  SELECT *
  FROM runner_saas_large_ext_gpu
  UNION ALL
  SELECT *
  FROM runner_saas_macos
  UNION ALL
  SELECT *
  FROM runner_saas_private
  UNION ALL
  SELECT *
  FROM runner_saas_private_ext
  UNION ALL
  SELECT *
  FROM haproxy_isp
  UNION ALL
  SELECT *
  FROM haproxy_inter
  UNION ALL
  SELECT *
  FROM haproxy_cdn
)

SELECT
  date_day,
  gcp_project_id,
  gcp_service_description,
  gcp_sku_description,
  infra_label,
  env_label,
  runner_label,
  folder_label,
  LOWER(pl_category)           AS pl_category,
  pl_percent,
  LISTAGG(DISTINCT from_mapping, ' || ') WITHIN GROUP (
    ORDER BY from_mapping ASC) AS from_mapping
FROM cte_append
{{ dbt_utils.group_by(n=10) }}