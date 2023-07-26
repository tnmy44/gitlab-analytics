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

env_labels AS (

  SELECT * FROM {{ ref('gcp_billing_export_resource_labels') }}
  WHERE resource_label_key = 'env'

),

runner_labels AS (

  SELECT
    source_primary_key,
    CASE
      WHEN resource_label_value LIKE '%runners-manager-shared-blue-%' AND resource_label_value NOT LIKE '%gpu%' THEN '2 - shared saas runners - small' --ok
      WHEN resource_label_value LIKE '%runners-manager-shared-green-%' AND resource_label_value NOT LIKE '%gpu%' THEN '2 - shared saas runners - small' --ok
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-large-amd64-green-%' AND resource_label_value NOT LIKE '%gpu%' THEN '4 - shared saas runners - large' --ok
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-medium-amd64-green-%' AND resource_label_value NOT LIKE '%gpu%' THEN '3 - shared saas runners - medium' --ok
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-medium-amd64-blue-%' AND resource_label_value NOT LIKE '%gpu%' THEN '3 - shared saas runners - medium' --ok
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-large-amd64-blue-%' AND resource_label_value NOT LIKE '%gpu%' THEN '4 - shared saas runners - large' --ok
      WHEN resource_label_value LIKE '%runners-manager-saas-macos-staging-green-%' AND resource_label_value NOT LIKE '%gpu%' THEN 'runners-manager-saas-macos-staging-green-'
      WHEN resource_label_value LIKE '%runners-manager-saas-macos-staging-blue-%' AND resource_label_value NOT LIKE '%gpu%' THEN 'runners-manager-saas-macos-staging-blue-'
      WHEN resource_label_value LIKE '%runners-manager-shared-gitlab-org-green-%' AND resource_label_value NOT LIKE '%gpu%' THEN '1 - shared gitlab org runners' --ok
      WHEN resource_label_value LIKE '%runners-manager-shared-gitlab-org-blue-%' AND resource_label_value NOT LIKE '%gpu%' THEN '1 - shared gitlab org runners' --ok
      WHEN resource_label_value LIKE '%runners-manager-private-blue-%' AND resource_label_value NOT LIKE '%gpu%' THEN '6 - private internal runners' --ok, project_pl internal
      WHEN resource_label_value LIKE '%runners-manager-private-green-%' AND resource_label_value NOT LIKE '%gpu%' THEN '6 - private internal runners' --ok, project_pl internal
      WHEN (resource_label_value LIKE '%instances/runner-%' AND resource_label_value LIKE '%shared-gitlab-org-%' AND resource_label_value NOT LIKE '%gpu%' ) THEN '1 - shared gitlab org runners' --ok
      WHEN (resource_label_value LIKE '%instances/runner-%' AND resource_label_value LIKE '%amd64%' AND resource_label_value NOT LIKE '%gpu%' ) THEN 'runners-saas'
      WHEN (resource_label_value LIKE '%instances/runner-%' AND resource_label_value LIKE '%s-shared-%' AND resource_label_value NOT LIKE '%gpu%' ) THEN '2 - shared saas runners - small' --ok
      WHEN (resource_label_value LIKE '%instances/runner-%' AND resource_label_value LIKE '%-shared-%' AND resource_label_value NOT LIKE '%gitlab%' AND resource_label_value NOT LIKE '%gpu%' ) THEN '2 - shared saas runners - small' --ok
      WHEN (resource_label_value LIKE '%instances/runner-%' AND resource_label_value LIKE '%-private-%' AND resource_label_value NOT LIKE '%gpu%' ) THEN '6 - private internal runners' --ok, project_pl internal
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-medium-%' AND resource_label_value LIKE '%gpu%' THEN '8 - shared saas runners gpu - medium'
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-large-%' AND resource_label_value LIKE '%gpu%' THEN '9 - shared saas runners gpu - large'
      WHEN resource_label_value LIKE '%gke-runners-gke-default-pool-%' THEN 'gke-runners-gke-default-pool-'
      WHEN resource_label_value LIKE '%test-machine-%' THEN 'test-machine-'
      WHEN resource_label_value LIKE '%tm-runner-%' THEN 'tm-runner-'
      WHEN resource_label_value LIKE '%tm-test-instance%' THEN 'tm-test-instance'
      WHEN resource_label_value LIKE '%gitlab-temporary-gcp-image-%' THEN 'gitlab-temporary-gcp-image-'
      WHEN resource_label_value LIKE '%sd-exporter%' THEN 'sd-exporter'
      WHEN resource_label_value LIKE '%/bastion-%' THEN 'bastion'
      WHEN resource_label_value LIKE '%/gitlab-qa-tunnel%' THEN 'gitlab-qa-tunnel'
      ELSE resource_label_value
    END AS resource_label_value
  FROM {{ ref('gcp_billing_export_resource_labels') }}
  WHERE resource_label_key = 'runner_manager_name'
),

unit_mapping AS (

  SELECT * FROM {{ ref('gcp_billing_unit_mapping') }}
  WHERE category = 'usage'

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
  bill.day                           AS day,
  COALESCE(bill.project_id, 'no_id') AS gcp_project_id,
  bill.service                       AS gcp_service_description,
  bill.sku_description               AS gcp_sku_description,
  bill.infra_label                   AS infra_label,
  bill.env_label                     AS env_label,
  bill.runner_label                  AS runner_label,
  bill.usage_unit,
  bill.pricing_unit,
  bill.usage_amount                  AS usage_amount,
  bill.usage_amount_in_pricing_units AS usage_amount_in_pricing_units,
  bill.cost_before_credits           AS cost_before_credits,
  bill.net_cost                      AS net_cost,
  usage.converted_unit               AS usage_standard_unit,
  bill.usage_amount / usage.rate     AS usage_amount_in_standard_unit
FROM billing_base AS bill
LEFT JOIN unit_mapping AS usage ON usage.raw_unit = bill.usage_unit
