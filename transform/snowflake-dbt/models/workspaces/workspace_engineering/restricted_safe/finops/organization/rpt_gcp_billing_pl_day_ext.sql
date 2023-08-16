WITH pl_day AS (

  SELECT *
  FROM {{ ref('rpt_gcp_billing_pl_day') }}

),

project_full_path AS (

  SELECT *
  FROM {{ ref('project_full_path') }}
  WHERE most_recent = TRUE

),
/*
For every new category breakout, a new cte is created to join with the previous one
*/
join_path AS (

  SELECT
    pd.*,
    pfp.full_path AS project_full_path
  FROM pl_day AS pd
  INNER JOIN project_full_path AS pfp ON pd.gcp_project_id = pfp.gcp_project_id

),

join_product_component AS (

  SELECT
    *,
    CASE
      WHEN from_mapping LIKE '%ci_runner_pl_daily%'
        OR from_mapping LIKE 'build_artifacts%'
        OR infra_label IN ('continuous_integration', 'runner_saas', 'build_artifacts') THEN 'Production: Continuous Integration'
      WHEN from_mapping LIKE '%haproxy-%' OR infra_label = 'pages' THEN 'Production: HAProxy'
      WHEN from_mapping LIKE 'repo_storage%' OR infra_label IN ('git_lfs', 'gitaly') THEN 'Production: Gitaly'
      WHEN from_mapping LIKE 'container_registry%' OR infra_label IN ('container_registry', 'registry') THEN 'Production: Container Registry'
      WHEN from_mapping IN ('folder_pl', 'projects_pl') OR infra_label = 'security' OR gcp_project_id LIKE 'gitlab-ci-private-%' OR infra_label = 'infrastructure' THEN 'R&D - Staging, Ops, Quality, Security, Demos, Sandboxes'
      WHEN gcp_project_id = 'unreview-poc-390200e5' THEN 'AI'
      WHEN infra_label = 'dependency_proxy' THEN 'Production: Dependency Proxy'
      WHEN infra_label = 'shared' AND from_mapping = 'infralabel_pl' THEN 'WIP: Unallocated Production costs'
      WHEN infra_label IS NULL AND from_mapping IS NULL THEN
        CASE WHEN gcp_project_id != 'gitlab-production' THEN 'WIP: Unallocated costs'
          WHEN gcp_project_id = 'gitlab-production' THEN 'WIP: Unallocated Production costs' END
      ELSE CONCAT(infra_label, '-', from_mapping)
    END AS product_component
  FROM join_path

)

/*
join_finance_component AS (

  SELECT
    *,
    CASE
      -- STORAGE
      WHEN LOWER(gcp_service_description) LIKE 'cloud storage'
        AND (LOWER(gcp_sku_description) LIKE '%standard storage%') THEN 'Storage'
      WHEN LOWER(gcp_service_description) LIKE 'cloud storage'
        AND (LOWER(gcp_sku_description) LIKE '%coldline storage%') THEN 'Storage'
      WHEN LOWER(gcp_service_description) LIKE 'cloud storage'
        AND (LOWER(gcp_sku_description) LIKE '%archive storage%') THEN 'Storage'
      WHEN LOWER(gcp_service_description) LIKE 'cloud storage'
        AND (LOWER(gcp_sku_description) LIKE '%nearline storage%') THEN 'Storage'
      WHEN LOWER(gcp_service_description) LIKE 'cloud storage'
        AND (LOWER(gcp_sku_description) LIKE '%operations%') THEN 'Storage'
      WHEN LOWER(gcp_service_description) LIKE 'compute engine'
        AND LOWER(gcp_sku_description) LIKE '%pd capacity%' THEN 'Storage'
      WHEN LOWER(gcp_service_description) LIKE 'compute engine'
        AND LOWER(gcp_sku_description) LIKE '%pd snapshot%' THEN 'Storage'
      WHEN (LOWER(gcp_service_description) LIKE 'bigquery'
        AND LOWER(gcp_sku_description) LIKE '%storage%') THEN 'Storage'
      WHEN (LOWER(gcp_service_description) LIKE 'cloud sql'
        AND LOWER(gcp_sku_description) LIKE '%storage%') THEN 'Storage'
      -- COMPUTE
      WHEN LOWER(gcp_sku_description) LIKE '%commitment%' THEN 'Committed Usage'
      WHEN LOWER(gcp_service_description) LIKE 'bigquery'
        AND LOWER(gcp_sku_description) NOT LIKE 'storage' THEN 'Compute'
      WHEN LOWER(gcp_service_description) LIKE 'cloud sql'
        AND (LOWER(gcp_sku_description) LIKE '%cpu%'
          OR LOWER(gcp_sku_description) LIKE '%ram%') THEN 'Compute'
      WHEN LOWER(gcp_service_description) LIKE 'kubernetes engine' THEN 'Compute'
      WHEN LOWER(gcp_service_description) LIKE 'vertex ai' THEN 'Compute'
      WHEN LOWER(gcp_service_description) LIKE 'compute engine'
        AND LOWER(gcp_sku_description) LIKE '%gpu%' THEN 'Compute'
      WHEN LOWER(gcp_service_description) LIKE '%memorystore%'
        AND LOWER(gcp_sku_description) LIKE '%capacity%' THEN 'Compute'
      WHEN LOWER(gcp_service_description) LIKE 'compute engine'
        AND (LOWER(gcp_sku_description) LIKE '%ram%'
          OR LOWER(gcp_sku_description) LIKE '%cpu%'
          OR LOWER(gcp_sku_description) LIKE '%core%') THEN 'Compute'
      -- NETWORKING
      WHEN LOWER(gcp_service_description) LIKE '%cloud sql%'
        AND LOWER(gcp_sku_description) LIKE '%networking%' THEN 'Networking'
      WHEN LOWER(gcp_service_description) LIKE '%cloud pub/sub%' THEN 'Networking'
      WHEN LOWER(gcp_service_description) LIKE '%memorystore%'
        AND LOWER(gcp_sku_description) LIKE '%networking%' THEN 'Networking'
      WHEN LOWER(gcp_service_description) LIKE '%cloud storage%'
        AND LOWER(gcp_sku_description) LIKE '%egress%' THEN 'Networking'
      WHEN LOWER(gcp_service_description) LIKE '%cloud storage%'
        AND LOWER(gcp_sku_description) LIKE '%cdn%' THEN 'Networking'
      WHEN LOWER(gcp_service_description) = 'networking' THEN 'Networking'
      WHEN LOWER(gcp_sku_description) LIKE '%load balanc%' THEN 'Networking'
      WHEN LOWER(gcp_service_description) LIKE '%compute engine%'
        AND LOWER(gcp_sku_description) LIKE '%ip%' THEN 'Networking'
      WHEN LOWER(gcp_service_description) LIKE '%compute engine%'
        AND LOWER(gcp_sku_description) LIKE '%network%' THEN 'Networking'
      WHEN LOWER(gcp_service_description) LIKE '%cloud storage%'
        AND (LOWER(gcp_sku_description) LIKE '%network%'
          OR LOWER(gcp_sku_description) LIKE '%download%') THEN 'Networking'
      -- LOGGING AND METRICS
      WHEN LOWER(gcp_service_description) LIKE 'stackdriver monitoring' THEN 'Logging and Metrics'
      WHEN LOWER(gcp_service_description) LIKE 'cloud logging' THEN 'Logging and Metrics'
      -- SUPPORT
      WHEN LOWER(gcp_sku_description) LIKE '%support%' THEN 'Support'
      WHEN LOWER(gcp_sku_description) LIKE '%security command center%' THEN 'Support'
      WHEN LOWER(gcp_sku_description) LIKE '%marketplace%' THEN 'Support'
      ELSE 'Other'
    END AS finance_sku_type,

    CASE
      -- STORAGE
      WHEN LOWER(gcp_service_description) LIKE 'cloud storage'
        AND (LOWER(gcp_sku_description) LIKE '%standard storage%') THEN 'Object (storage)'
      WHEN LOWER(gcp_service_description) LIKE 'cloud storage'
        AND (LOWER(gcp_sku_description) LIKE '%coldline storage%') THEN 'Object (storage)'
      WHEN LOWER(gcp_service_description) LIKE 'cloud storage'
        AND (LOWER(gcp_sku_description) LIKE '%archive storage%') THEN 'Object (storage)'
      WHEN LOWER(gcp_service_description) LIKE 'cloud storage'
        AND (LOWER(gcp_sku_description) LIKE '%nearline storage%') THEN 'Object (storage)'
      WHEN LOWER(gcp_service_description) LIKE 'cloud storage'
        AND (LOWER(gcp_sku_description) LIKE '%operations%') THEN 'Object (operations)'
      WHEN LOWER(gcp_service_description) LIKE 'compute engine'
        AND LOWER(gcp_sku_description) LIKE '%pd capacity%' THEN 'Repository (storage)'
      WHEN LOWER(gcp_service_description) LIKE 'compute engine'
        AND LOWER(gcp_sku_description) LIKE '%pd snapshot%' THEN 'Repository (storage)'
      WHEN (LOWER(gcp_service_description) LIKE 'bigquery'
        AND LOWER(gcp_sku_description) LIKE '%storage%') THEN 'Data Warehouse (storage)'
      WHEN (LOWER(gcp_service_description) LIKE 'cloud sql'
        AND LOWER(gcp_sku_description) LIKE '%storage%') THEN 'Databases (storage)'
      -- COMPUTE
      WHEN LOWER(gcp_sku_description) LIKE '%commitment%' THEN 'Committed Usage'
      WHEN LOWER(gcp_service_description) LIKE 'bigquery'
        AND LOWER(gcp_sku_description) NOT LIKE 'storage' THEN 'Data Warehouse (compute)'
      WHEN LOWER(gcp_service_description) LIKE 'cloud sql'
        AND (LOWER(gcp_sku_description) LIKE '%cpu%'
          OR LOWER(gcp_sku_description) LIKE '%ram%') THEN 'Data Warehouse (compute)'
      WHEN LOWER(gcp_service_description) LIKE 'kubernetes engine' THEN 'Container orchestration (compute)'
      WHEN LOWER(gcp_service_description) LIKE 'vertex ai' THEN 'AI/ML (compute)'
      WHEN LOWER(gcp_service_description) LIKE 'compute engine'
        AND LOWER(gcp_sku_description) LIKE '%gpu%' THEN 'AI/ML (compute)'
      WHEN LOWER(gcp_service_description) LIKE '%memorystore%'
        AND LOWER(gcp_sku_description) LIKE '%capacity%' THEN 'Memorystores (compute)'
      WHEN LOWER(gcp_service_description) LIKE 'compute engine'
        AND (LOWER(gcp_sku_description) LIKE '%ram%'
          OR LOWER(gcp_sku_description) LIKE '%cpu%'
          OR LOWER(gcp_sku_description) LIKE '%core%') THEN 'Containers (compute)'
      -- NETWORKING
      WHEN LOWER(gcp_service_description) LIKE '%cloud sql%'
        AND LOWER(gcp_sku_description) LIKE '%networking%' THEN 'Databases (networking)'
      WHEN LOWER(gcp_service_description) LIKE '%cloud pub/sub%' THEN 'Messaging (networking)'
      WHEN LOWER(gcp_service_description) LIKE '%memorystore%'
        AND LOWER(gcp_sku_description) LIKE '%networking%' THEN 'Memorystores (networking)'
      WHEN LOWER(gcp_service_description) LIKE '%cloud storage%'
        AND LOWER(gcp_sku_description) LIKE '%egress%' THEN 'Object (networking)'
      WHEN LOWER(gcp_service_description) LIKE '%cloud storage%'
        AND LOWER(gcp_sku_description) LIKE '%cdn%' THEN 'Object CDN (networking)'
      WHEN LOWER(gcp_service_description) = 'networking' THEN 'Networking (mixed)'
      WHEN LOWER(gcp_sku_description) LIKE '%load balanc%' THEN 'Load Balancing (networking)'
      WHEN LOWER(gcp_service_description) LIKE '%compute engine%'
        AND LOWER(gcp_sku_description) LIKE '%ip%' THEN 'Container networking (networking)'
      WHEN LOWER(gcp_service_description) LIKE '%compute engine%'
        AND LOWER(gcp_sku_description) LIKE '%network%' THEN 'Container networking (networking)'
      WHEN LOWER(gcp_service_description) LIKE '%cloud storage%'
        AND (LOWER(gcp_sku_description) LIKE '%network%'
          OR LOWER(gcp_sku_description) LIKE '%download%') THEN 'Container networking (networking)'
      -- LOGGING AND METRICS
      WHEN LOWER(gcp_service_description) LIKE 'stackdriver monitoring' THEN 'Metrics (logging and metrics)'
      WHEN LOWER(gcp_service_description) LIKE 'cloud logging' THEN 'Logging (logging and metrics)'
      -- SUPPORT
      WHEN LOWER(gcp_sku_description) LIKE '%support%' THEN 'Google Support (support)'
      WHEN LOWER(gcp_sku_description) LIKE '%security command center%' THEN 'Security (support)'
      WHEN LOWER(gcp_sku_description) LIKE '%marketplace%' THEN 'Marketplace (support)'
      ELSE 'Other'
    END AS finance_sku_subtype
  FROM join_product_component
  
)*/

SELECT * FROM join_product_component
