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
  {{ dbt_utils.group_by(n=8) }}
)

SELECT
  bill.day                                AS day,
  bill.project_id                         AS gcp_project_id,
  bill.service                            AS gcp_service_description,
  bill.sku_description                    AS gcp_sku_description,
  bill.infra_label                        AS infra_label,
  bill.env_label                          AS env_label,
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

