{{ config(
    materialized='table',
    )
}}


WITH export AS (

  SELECT * FROM {{ ref('gcp_billing_export_xf') }}
  WHERE invoice_month >= '2022-01-01'

),

infra_labels AS (

  SELECT * FROM {{ ref('gcp_billing_export_resource_labels') }}
  WHERE resource_label_key = 'gl_product_category'

),

env_labels as (

  SELECT * FROM {{ ref('gcp_billing_export_resource_labels') }}
  WHERE resource_label_key = 'env'

),

runner_labels as (

  SELECT 
  source_primary_key,
  case 
    when resource_label_value like '%runners-manager-shared-blue-%' then '2 - shared saas runners - small' --ok
    when resource_label_value like '%runners-manager-shared-green-%' then '2 - shared saas runners - small' --ok
    when resource_label_value like '%runners-manager-saas-linux-large-amd64-green-%' then 'runners-manager-saas-linux-large-amd64-green-'
    when resource_label_value like '%runners-manager-saas-linux-medium-amd64-green-%' then '3 - shared saas runners - medium' --ok
    when resource_label_value like '%runners-manager-saas-linux-medium-amd64-blue-%' then '3 - shared saas runners - medium' --ok
    when resource_label_value like '%runners-manager-saas-linux-large-amd64-blue-%' then 'runners-manager-saas-linux-large-amd64-blue-'
    when resource_label_value like '%runners-manager-saas-macos-staging-green-%' then 'runners-manager-saas-macos-staging-green-'
    when resource_label_value like '%runners-manager-saas-macos-staging-blue-%' then 'runners-manager-saas-macos-staging-blue-'
    when resource_label_value like '%runners-manager-shared-gitlab-org-green-%' then '1 - shared gitlab org runners' --ok
    when resource_label_value like '%runners-manager-shared-gitlab-org-blue-%' then '1 - shared gitlab org runners' --ok
    when resource_label_value like '%runners-manager-private-blue-%' then 'runners-manager-private-blue-'
    when resource_label_value like '%runners-manager-private-green-%' then 'runners-manager-private-green-'
    when (resource_label_value like '%instances/runner-%' and resource_label_value like '%shared-gitlab-org-%') then '1 - shared gitlab org runners' --ok
    when (resource_label_value like '%instances/runner-%' and resource_label_value like '%amd64%') then 'runners-saas'
    when (resource_label_value like '%instances/runner-%' and resource_label_value like '%s-shared-%') then '2 - shared saas runners - small' --ok
    when (resource_label_value like '%instances/runner-%' and resource_label_value like '%-shared-%' and resource_label_value not like '%gitlab%') then '2 - shared saas runners - small' --ok
    when (resource_label_value like '%instances/runner-%' and resource_label_value like '%-private-%') then 'runners-private'
    when resource_label_value like '%gke-runners-gke-default-pool-%' then 'gke-runners-gke-default-pool-'
    when resource_label_value like '%test-machine-%' then 'test-machine-'
    when resource_label_value like '%tm-runner-%' then 'tm-runner-'
    when resource_label_value like '%tm-test-instance%' then 'tm-test-instance'
    when resource_label_value like '%gitlab-temporary-gcp-image-%' then 'gitlab-temporary-gcp-image-'
    when resource_label_value like '%sd-exporter%' then 'sd-exporter'
    when resource_label_value like '%/bastion-%' then 'bastion'
    when resource_label_value like '%/gitlab-qa-tunnel%' then 'gitlab-qa-tunnel'
  else resource_label_value
  end as resource_label_value
  FROM {{ ref('gcp_billing_export_resource_labels') }}
  WHERE resource_label_key = 'runner_manager_name'

),

machine_type as (

  SELECT 
  source_primary_key,
  case 
    when resource_label_value like '%n1-standard-1%' then 'linux small - n1-standard-1'
    when resource_label_value like '%n2d-standard-2%' then 'linux medium - n2d-standard-2'
    when resource_label_value like '%n2d-standard-4%' then 'linux large - n2d-standard-4'
    when resource_label_value like '%n1-standard-4%' then 'linux mlops - n1-standard-4-gpu'
    when resource_label_value like '%c2-standard-30%' then 'linux shared - c2-standard-30'
    when resource_label_value like '%n1-standard-2%' then 'linux medium - n1-standard-2'
    when resource_label_value like '%e2-standard-16%' then 'e2-standard-16'
    when resource_label_value like '%e2-standard-4%' then 'e2-standard-4'
    else resource_label_value
  end as resource_label_value
  FROM {{ ref('gcp_billing_export_resource_labels') }}
  WHERE resource_label_key = 'machine_type'

),


billing_base AS (
  SELECT
    DATE(export.usage_start_time)             AS day,
    export.project_id                         AS project_id,
    export.service_description                AS service,
    export.sku_description                    AS sku_description,
    infra_labels.resource_label_value         AS infra_label,
    env_labels.resource_label_value           AS env_label,
    runner_labels.resource_label_value        AS runner_label,
    machine_type.resource_label_value         AS machine_type,
    export.usage_unit                         AS usage_unit,
    export.pricing_unit                       AS pricing_unit,
    SUM(export.usage_amount)                  AS usage_amount,
    SUM(export.usage_amount_in_pricing_units) AS usage_amount_in_pricing_units,
    SUM(export.cost_before_credits)           AS cost_before_credits,
    SUM(export.total_cost)                    AS net_cost
  FROM
    export
  LEFT JOIN
    infra_labels
    ON
      export.source_primary_key = infra_labels.source_primary_key
  LEFT JOIN
    env_labels
    ON
      export.source_primary_key = env_labels.source_primary_key
  LEFT JOIN
    runner_labels
    ON
      export.source_primary_key = runner_labels.source_primary_key
  LEFT JOIN
    machine_type
    ON
      export.source_primary_key = machine_type.source_primary_key
  {{ dbt_utils.group_by(n=10) }}
)

SELECT
  billing_base.day                                AS day,
  billing_base.project_id                         AS gcp_project_id,
  billing_base.service                            AS gcp_service_description,
  billing_base.sku_description                    AS gcp_sku_description,
  billing_base.infra_label                        AS infra_label,
  billing_base.env_label                          AS env_label,
  billing_base.runner_label                       AS runner_label,
  billing_base.machine_type                       AS machine_type,
  billing_base.usage_unit,
  billing_base.pricing_unit,
  SUM(billing_base.usage_amount)                  AS usage_amount,
  SUM(billing_base.usage_amount_in_pricing_units) AS usage_amount_in_pricing_units,
  SUM(billing_base.cost_before_credits)           AS cost_before_credits,
  SUM(billing_base.net_cost)                      AS net_cost
FROM billing_base
{{ dbt_utils.group_by(n=10) }}
