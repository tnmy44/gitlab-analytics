{{ config(
    materialized='table',
    )
}}


WITH service_base AS (

  SELECT * FROM {{ ref('rpt_gcp_billing_infra_mapping_day') }}


),

infra_allocation as (

  SELECT * FROM {{ ref('infralabel_pl') }}
  unpivot(allocation for type in (free, internal, paid))

),

sandbox as (

  SELECT * FROM {{ ref('sandbox_projects_pl') }}

),

project_pl as (

  SELECT * FROM {{ ref('projects_pl') }}
    unpivot(allocation for type in (free, internal, paid))

),

repo_size AS (

    SELECT * FROM {{ ref('repo_storage_pl_daily')}}
)

SELECT
service_base.day,
service_base.gcp_project_id,
service_base.gcp_service_description,
service_base.gcp_sku_description,
service_base.infra_label,
lower(coalesce(sandbox.classification, project_pl.type, infra_allocation.type, 'unknown')) as finance_pl,
service_base.usage_unit as usage_unit,
service_base.pricing_unit as pricing_unit,
-- usage amount
service_base.usage_amount * coalesce(infra_allocation.allocation, project_pl.allocation, 1)  as usage_amount,
-- usage amount in p unit
service_base.usage_amount_in_pricing_units * coalesce(infra_allocation.allocation, project_pl.allocation, 1)  as usage_amount_in_pricing_units,
-- cost before discounts
service_base.cost_before_credits * coalesce(infra_allocation.allocation, project_pl.allocation, 1) as cost_before_credits,
-- net costs
service_base.net_cost * coalesce(infra_allocation.allocation, project_pl.allocation, 1) as net_cost
FROM
service_base
LEFT JOIN infra_allocation on infra_allocation.infra_label = service_base.infra_label
LEFT JOIN sandbox on sandbox.project_name = service_base.gcp_project_id
LEFT JOIN project_pl on project_pl.project_id = service_base.gcp_project_id
where net_cost != 0

