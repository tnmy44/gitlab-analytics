WITH date_spine AS (

  SELECT date_day FROM {{ ref('dim_date') }}
  WHERE date_day < GETDATE() AND date_day >= '2020-01-01'
),

infralabel_pl AS (


  SELECT
    date_spine.date_day,
    NULL                     AS gcp_project_id,
    NULL                     AS gcp_service_description,
    NULL                     AS gcp_sku_description,
    infralabel_pl.infra_label,
    lower(infralabel_pl.type)       AS pl_category,
    infralabel_pl.allocation AS pl_percent,
    'infralabel_pl'          AS from_mapping
  FROM date_spine
  CROSS JOIN {{ ref('infralabel_pl') }}

),

projects_pl AS (

  SELECT
    date_spine.date_day,
    projects_pl.project_id AS gcp_project_id,
    NULL                   AS gcp_service_description,
    NULL                   AS gcp_sku_description,
    NULL                   AS infra_label,
    lower(projects_pl.type)       AS pl_category,
    projects_pl.allocation AS pl_percent,
    'projects_pl'          AS from_mapping
  FROM date_spine
  CROSS JOIN {{ ref ('projects_pl') }}

),

repo_storage_pl_daily AS (

  SELECT
    snapshot_day                               AS date_day,
    'gitlab-production'                        AS gcp_project_id,
    'Compute Engine'                           AS gcp_service_description,
    'SSD backed PD Capacity'                   AS gcp_sku_description,
    'gitaly'                                   AS infra_label,
    lower(repo_storage_pl_daily.finance_pl)           AS pl_category,
    repo_storage_pl_daily.percent_repo_size_gb AS pl_percent,
    'repo_storage_pl_daily'                    AS from_mapping
  FROM {{ ref ('repo_storage_pl_daily') }}
),

sandbox_projects_pl AS (

  SELECT
    date_spine.date_day,
    sandbox_projects_pl.project_name   AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    lower(sandbox_projects_pl.classification) AS pl_category,
    1                                  AS pl_percent,
    'sandbox_projects_pl'              AS from_mapping
  FROM date_spine
  CROSS JOIN {{ ref ('sandbox_projects_pl') }}
),

container_registry_pl_daily AS (

  SELECT
    snapshot_day                               AS date_day,
    'gitlab-production'                        AS gcp_project_id,
    'Cloud Storage'                           AS gcp_service_description,
    'Standard Storage US Multi-region'        AS gcp_sku_description,
    'registry'                                   AS infra_label,
    lower(container_registry_pl_daily.finance_pl)           AS pl_category,
    container_registry_pl_daily.percent_container_registry_size AS pl_percent,
    'container_registry_pl_daily'                    AS from_mapping
  FROM {{ ref ('container_registry_pl_daily') }}
  where snapshot_day > '2022-06-10'

)

SELECT * FROM infralabel_pl
UNION ALL
SELECT * FROM projects_pl
UNION ALL
SELECT * FROM repo_storage_pl_daily
UNION ALL
SELECT * FROM sandbox_projects_pl
UNION ALL
SELECT * FROM container_registry_pl_daily