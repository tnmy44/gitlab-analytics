{{ config(
    materialized='table',
    )
}}


WITH export AS (

  SELECT * FROM {{ ref('gcp_billing_export_xf') }}
  WHERE invoice_month >= '2022-12-01'

),

infra_labels as (

  SELECT * FROM {{ ref('gcp_billing_export_resource_labels') }}
  where resource_label_key = 'gl_product_category'

),


  billing_base as (
        SELECT
        date(export.usage_start_time) AS day,
        export.project_id AS project_id,
        export.service_description AS service,
        export.sku_description AS sku_description,
        infra_labels.resource_label_value as infra_label,
        export.usage_unit as usage_unit,
        export.pricing_unit as pricing_unit,
        -- usage amount
        sum(export.usage_amount) as usage_amount,
        -- usage amount in p unit
        sum(export.usage_amount_in_pricing_units) as usage_amount_in_pricing_units,
        -- cost before discounts
        sum(export.cost_before_credits) as cost_before_credits,
        -- net costs
        sum(export.total_cost) AS net_cost
        FROM
        export
        LEFT JOIN
        infra_labels
        ON
        export.source_primary_key = infra_labels.source_primary_key
        -- -- -- -- -- -- 
       {{ dbt_utils.group_by(n=7) }})

SELECT
        billing_base.day as day,
        billing_base.project_id as gcp_project_id,
        billing_base.service as gcp_service_description,
        billing_base.sku_description as gcp_sku_description,
        billing_base.infra_label as infra_label,
        billing_base.usage_unit,
        billing_base.pricing_unit,
        sum(billing_base.usage_amount) as usage_amount,
        sum(billing_base.usage_amount_in_pricing_units) as usage_amount_in_pricing_units,
        sum(billing_base.cost_before_credits) as cost_before_credits,
        sum(billing_base.net_cost) as net_cost
FROM billing_base 
       {{ dbt_utils.group_by(n=7) }}
