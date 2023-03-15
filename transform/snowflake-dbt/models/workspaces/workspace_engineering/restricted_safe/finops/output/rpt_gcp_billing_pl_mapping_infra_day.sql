{{ config(
    materialized='table',
    )
}}


WITH service_base AS (

  SELECT * FROM {{ ref('rpt_gcp_billing_infra_mapping_day') }}


),

infra_allocation AS (

  SELECT * FROM {{ ref('infralabel_pl') }}

),

sandbox AS (

  SELECT * FROM {{ ref('sandbox_projects_pl') }}

),

project_pl AS (

  SELECT * FROM {{ ref('projects_pl') }}


),

repo_size AS (

  SELECT * FROM {{ ref('repo_storage_pl_daily') }}
)

SELECT
  service_base.day,
  service_base.gcp_project_id,
  service_base.gcp_service_description,
  service_base.gcp_sku_description,
  service_base.infra_label,
  LOWER(COALESCE(sandbox.classification, project_pl.type, infra_allocation.type, 'unknown'))                   AS finance_pl,
  service_base.usage_unit                                                                                      AS usage_unit,
  service_base.pricing_unit                                                                                    AS pricing_unit,
  -- usage amount
  service_base.usage_amount * COALESCE(infra_allocation.allocation, project_pl.allocation, 1)                  AS usage_amount,
  -- usage amount in p unit
  service_base.usage_amount_in_pricing_units * COALESCE(infra_allocation.allocation, project_pl.allocation, 1) AS usage_amount_in_pricing_units,
  -- cost before discounts
  service_base.cost_before_credits * COALESCE(infra_allocation.allocation, project_pl.allocation, 1)           AS cost_before_credits,
  -- net costs
  service_base.net_cost * COALESCE(infra_allocation.allocation, project_pl.allocation, 1)                      AS net_cost
FROM
  service_base
LEFT JOIN infra_allocation ON infra_allocation.infra_label = service_base.infra_label
LEFT JOIN sandbox ON sandbox.project_name = service_base.gcp_project_id
LEFT JOIN project_pl ON project_pl.project_id = service_base.gcp_project_id
WHERE net_cost != 0
