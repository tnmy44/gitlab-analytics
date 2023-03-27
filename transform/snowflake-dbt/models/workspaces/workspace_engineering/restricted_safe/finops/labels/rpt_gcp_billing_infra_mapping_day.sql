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


billing_base AS (
  SELECT
    DATE(export.usage_start_time)             AS day,
    export.project_id                         AS project_id,
    export.service_description                AS service,
    export.sku_description                    AS sku_description,
    infra_labels.resource_label_value         AS infra_label,
    export.usage_unit                         AS usage_unit,
    export.pricing_unit                       AS pricing_unit,
    -- usage amount
    SUM(export.usage_amount)                  AS usage_amount,
    -- usage amount in p unit
    SUM(export.usage_amount_in_pricing_units) AS usage_amount_in_pricing_units,
    -- cost before discounts
    SUM(export.cost_before_credits)           AS cost_before_credits,
    -- net costs
    SUM(export.total_cost)                    AS net_cost
  FROM
    export
  LEFT JOIN
    infra_labels
    ON
      export.source_primary_key = infra_labels.source_primary_key
  -- -- -- -- -- -- 
  {{ dbt_utils.group_by(n=7) }}
)

SELECT
  billing_base.day                                AS day,
  billing_base.project_id                         AS gcp_project_id,
  billing_base.service                            AS gcp_service_description,
  billing_base.sku_description                    AS gcp_sku_description,
  billing_base.infra_label                        AS infra_label,
  billing_base.usage_unit,
  billing_base.pricing_unit,
  SUM(billing_base.usage_amount)                  AS usage_amount,
  SUM(billing_base.usage_amount_in_pricing_units) AS usage_amount_in_pricing_units,
  SUM(billing_base.cost_before_credits)           AS cost_before_credits,
  SUM(billing_base.net_cost)                      AS net_cost
FROM billing_base
{{ dbt_utils.group_by(n=7) }}
