{{ config(
    materialized='table',
    )
}}


WITH service_base AS (

  SELECT * FROM {{ ref('rpt_gcp_billing_infra_mapping_day') }}


),

combined_pl_mapping as (

SELECT * FROM {{ ref('combined_pl_mapping') }}

)

SELECT
  {# service_base.day,
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
  service_base.net_cost * COALESCE(infra_allocation.allocation, project_pl.allocation, 1)                      AS net_cost #}
  *
FROM
  service_base
LEFT JOIN combined_pl_mapping ON combined_pl_mapping.date_day = service_base.day
AND coalesce(combined_pl_mapping.gcp_project_id, service_base.gcp_project_id) = service_base.gcp_project_id
AND coalesce(combined_pl_mapping.gcp_service_description, service_base.gcp_service_description) = service_base.gcp_service_description
AND coalesce(combined_pl_mapping.gcp_sku_description, service_base.gcp_sku_description) = service_base.gcp_sku_description
AND coalesce(combined_pl_mapping.infra_label, service_base.infra_label) = service_base.infra_label
WHERE net_cost != 0
{# and pl_category is not null #}
{# and service_base.gcp_sku_description = 'SSD backed PD Capacity'
and combined_pl_mapping.infra_label = 'gitaly' #}


SELECT * FROM "CLEROUX_PROD".restricted_safe_workspace_engineering.combined_pl_mapping where gcp_project_id like '%staging%'