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
    when resource_label_value like '%runners-manager-saas-linux-large-amd64-green-%' then '4 - shared saas runners - large' --ok
    when resource_label_value like '%runners-manager-saas-linux-medium-amd64-green-%' then '3 - shared saas runners - medium' --ok
    when resource_label_value like '%runners-manager-saas-linux-medium-amd64-blue-%' then '3 - shared saas runners - medium' --ok
    when resource_label_value like '%runners-manager-saas-linux-large-amd64-blue-%' then '4 - shared saas runners - large' --ok
    when resource_label_value like '%runners-manager-saas-macos-staging-green-%' then 'runners-manager-saas-macos-staging-green-'
    when resource_label_value like '%runners-manager-saas-macos-staging-blue-%' then 'runners-manager-saas-macos-staging-blue-'
    when resource_label_value like '%runners-manager-shared-gitlab-org-green-%' then '1 - shared gitlab org runners' --ok
    when resource_label_value like '%runners-manager-shared-gitlab-org-blue-%' then '1 - shared gitlab org runners' --ok
    when resource_label_value like '%runners-manager-private-blue-%' then '6 - private internal runners' --ok, project_pl internal
    when resource_label_value like '%runners-manager-private-green-%' then '6 - private internal runners' --ok, project_pl internal
    when (resource_label_value like '%instances/runner-%' and resource_label_value like '%shared-gitlab-org-%') then '1 - shared gitlab org runners' --ok
    when (resource_label_value like '%instances/runner-%' and resource_label_value like '%amd64%') then 'runners-saas'
    when (resource_label_value like '%instances/runner-%' and resource_label_value like '%s-shared-%') then '2 - shared saas runners - small' --ok
    when (resource_label_value like '%instances/runner-%' and resource_label_value like '%-shared-%' and resource_label_value not like '%gitlab%') then '2 - shared saas runners - small' --ok
    when (resource_label_value like '%instances/runner-%' and resource_label_value like '%-private-%') then '6 - private internal runners' --ok, project_pl internal
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

unit_mapping as (

  SELECT * FROM {{ ref('gcp_billing_unit_mapping') }}
  WHERE category='usage'

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
  {{ dbt_utils.group_by(n=9) }}
)

SELECT
  bill.day                                AS day,
  coalesce(bill.project_id, 'no_id')       AS gcp_project_id,
  bill.service                            AS gcp_service_description,
  bill.sku_description                    AS gcp_sku_description,
  bill.infra_label                        AS infra_label,
  bill.env_label                          AS env_label,
  bill.runner_label                       AS runner_label,
  bill.usage_unit,
  bill.pricing_unit,
  bill.usage_amount                  AS usage_amount,
  bill.usage_amount_in_pricing_units AS usage_amount_in_pricing_units,
  bill.cost_before_credits           AS cost_before_credits,
  bill.net_cost                      AS net_cost,
  usage.converted_unit               AS usage_standard_unit,
  bill.usage_amount / usage.rate     AS usage_amount_in_standard_unit
FROM billing_base as bill
LEFT JOIN unit_mapping as usage on usage.raw_unit = bill.usage_unit
