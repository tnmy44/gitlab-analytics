-- namespace classification snippet
{{ config(
    materialized='table',
    )
}}


WITH namespace_classification AS (

  SELECT * FROM {{ ref('rpt_product_namespace_classification') }}

),

projects as (

  SELECT * FROM {{ ref('gitlab_dotcom_projects_xf') }}

),

project_statistics AS 
(

  SELECT * FROM {{ ref('gitlab_dotcom_project_statistics') }}

),
namespaces_child AS 
(

  SELECT * FROM {{ ref('gitlab_dotcom_namespaces_xf') }}

),
s AS
(

SELECT * FROM {{ ref('gitlab_dotcom_namespace_root_storage_statistics')}}

),
am as 
(

SELECT * FROM {{ref('mart_ci_runner_activity_daily')}}

),
ns as 
(

SELECT * FROM {{ ref('dim_namespace')}}

),

cir as (

SELECT * FROM {{ ref('dim_ci_runner')}}

)



storage as 
(select namespace_ultimate_parent_id as namespace_id
 , max(projects.last_activity_at::date) as ns_last_activity
           , sum(coalesce(project_statistics.storage_size,0) / POW(1024,3)) AS storage_size_GB 
             , sum(coalesce(project_statistics.repository_size,0) / POW(1024,3)) AS repo_size_GB 
             , sum(coalesce(project_statistics.lfs_objects_size,0) / POW(1024,3)) AS lfs_size_GB 
             , sum(coalesce(project_statistics.WIKI_SIZE,0) / POW(1024,3)) AS WIKI_SIZE_GB
             , sum(coalesce(project_statistics.PACKAGES_SIZE,0) / POW(1024,3)) AS PACKAGES_SIZE_GB
             , sum(coalesce(project_statistics.build_artifacts_size,0) /POW(1024,3)) AS build_artifacts_size_GB
             , sum(coalesce(project_statistics.CONTAINER_REGISTRY_SIZE,0) / POW(1024,3)) AS container_registry_size_GB_from_projects 
 , count(distinct projects.project_id) as num_projects 
 , count(*) as cnt
 , max(coalesce(s.CONTAINER_REGISTRY_SIZE,0)/ POW(1024,3)) as CONTAINER_REGISTRY_SIZE_GB_from_root_namespace
  ,(repo_size_GB  +  build_artifacts_size_GB + CONTAINER_REGISTRY_SIZE_GB_from_root_namespace + PACKAGES_SIZE_GB + lfs_size_GB + WIKI_SIZE_GB) as overall_storage_size
  FROM  projects
    left JOIN project_statistics
    ON projects.project_id = project_statistics.project_id
    INNER JOIN namespaces_child
    ON projects.namespace_id = namespaces_child.namespace_id
 left join s on s.namespace_id = namespace_ultimate_parent_id -- unique at namespace_id 
 --where coalesce(project_statistics.storage_size,0) > 0 
                group by 1 ), 
                
ci as 
(SELECT am.ultimate_parent_namespace_id  as ns_id , 
CASE 
      WHEN cir.ci_runner_description LIKE 'private-runners-manager%' THEN 'private-runners-manager'
      WHEN cir.ci_runner_description LIKE 'gitlab-shared-runners-manager%' THEN 'gitlab-shared-runners-manager'
      WHEN cir.ci_runner_description LIKE 'shared-runners-manager%' THEN 'shared-runners-manager'
    -- n1-standard-2 ($.095/hour) + $.04/hour due to windows licensing fee
      WHEN cir.ci_runner_description LIKE 'windows-shared-runners-manager%' THEN 'windows-shared-runners-manager'
      ELSE 'others' end AS runner_cost_type,
SUM(ci_build_duration_in_s)/3600 AS ci_hrs
FROM am 
inner JOIN ns on ns.dim_namespace_id = am.ultimate_parent_namespace_id 
INNER JOIN ccir ON am.dim_ci_runner_id = cir.dim_ci_runner_id
WHERE is_paid_by_gitlab=TRUE
--AND cir.ci_runner_type=1
{# and report_date >= [daterange_start]
 and report_date <= [daterange_end] #}
GROUP BY 1,2 ) 

  select a.namespace_type_details, 
  a.namespace_type
  , count(distinct a.dim_namespace_id) as num_ns 
  , sum(repo_size_GB) as repo_size_GB
, sum(lfs_size_GB) as lfs_size_GB
, sum(WIKI_SIZE_GB) as WIKI_SIZE_GB
, sum(PACKAGES_SIZE_GB) as PACKAGES_SIZE_GB
, sum(build_artifacts_size_GB) as build_artifacts_size_GB
, sum(CONTAINER_REGISTRY_SIZE_GB_from_root_namespace) as CONTAINER_REGISTRY_SIZE_GB
, sum(ci_hrs) as ci_hrs
  from namespace_type a 
  left join storage s on s.namespace_id = a.dim_namespace_id 
  left join ci on ci.ns_id = a.dim_namespace_id 
  group by 1,2 
  order by 2,1