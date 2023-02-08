{{ config(
    materialized='table',
    )
}}


WITH service_base AS (

  SELECT * FROM {{ ref('rpt_gcp_billing_infra_mapping_day') }}


),

infra_allocation as (

  SELECT * FROM {{ ref('gcp_billing_infra_pl_mapping') }}
  unpivot(allocation for type in (free, internal, paid))

),

sandbox as (

  SELECT * FROM {{ ref('gcp_billing_sandbox_projects') }}

),

project_pl as (

  SELECT * FROM {{ ref('gcp_billing_project_pl_mapping') }}
    unpivot(allocation for type in (free, internal, paid))

)

SELECT
service_base.day,
service_base.gcp_project_id,
service_base.gcp_service_description,
service_base.gcp_sku_description,
service_base.infra_label,
lower(coalesce(sandbox.classification, project_pl.type, infra_allocation.type, 'unknown')) as finance_pl,
service_base.net_cost * coalesce(infra_allocation.allocation, project_pl.allocation, 1) as net_cost
FROM
service_base
LEFT JOIN infra_allocation on infra_allocation.infra_label = service_base.infra_label
LEFT JOIN sandbox on sandbox.project_name = service_base.gcp_project_id
LEFT JOIN project_pl on project_pl.project_id = service_base.gcp_project_id
where net_cost != 0

