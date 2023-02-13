{{ config(
    materialized='table',
    )
}}


WITH service_base AS (

  SELECT * FROM {{ ref('rpt_gcp_billing_service_mapping_day') }}


),

allocation as (

  SELECT * FROM {{ ref('gcp_billing_service_allocation') }}
  unpivot(allocation for type in (free, internal, paid))

),

sandbox as (

  SELECT * FROM {{ ref('gcp_billing_sandbox_projects') }}

)

SELECT
    service_base.day,
    service_base.gcp_project_id,
    service_base.gcp_service_description,
    service_base.gcp_sku_description,
    service_base.gitlab_service,
    lower(coalesce(allocation.type, sandbox.classification, 'unknown')) as finance_pl,
    service_base.usage_unit as usage_unit,
    service_base.pricing_unit as pricing_unit,
    -- usage amount
    service_base.usage_amount * coalesce(allocation.allocation, 1) as usage_amount,
    -- usage amount in p unit
    service_base.usage_amount_in_pricing_units * coalesce(allocation.allocation, 1) as usage_amount_in_pricing_units,
    -- cost before discounts
    service_base.cost_before_credits * coalesce(allocation.allocation, 1) as cost_before_credits,
    service_base.net_cost * coalesce(allocation.allocation, 1) as net_cost
FROM
service_base
    LEFT JOIN allocation on allocation.service = service_base.gitlab_service
    LEFT JOIN sandbox on sandbox.project_name = service_base.gcp_project_id

