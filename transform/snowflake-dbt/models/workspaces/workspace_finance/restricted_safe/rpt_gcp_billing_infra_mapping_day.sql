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
        -- net costs
        sum(export.total_cost) AS net_cost
        FROM
        export
        LEFT JOIN
        infra_labels
        ON
        export.source_primary_key = infra_labels.source_primary_key
        -- -- -- -- -- -- 
       {{ dbt_utils.group_by(n=5) }})

SELECT
        billing_base.day as day,
        billing_base.project_id as gcp_project_id,
        billing_base.service as gcp_service_description,
        billing_base.sku_description as gcp_sku_description,
        billing_base.infra_label as infra_label,
        sum(billing_base.net_cost) as net_cost
FROM billing_base 
group by 1, 2, 3, 4, 5
