{{ config(
    materialized='table',
    )
}}


WITH export AS (

  SELECT * FROM {{ ref('gcp_billing_export_xf') }}
  WHERE invoice_month >= '2022-01-01'

)

SELECT
  DATE(export.usage_end_time)               AS day,
  export.project_id                         AS gcp_project_id,
  export.service_description                AS gcp_service_description,
  export.sku_description                    AS gcp_sku_description,
  CASE
    -- STORAGE
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        LOWER(gcp_sku_description) LIKE '%standard storage%'
      ) THEN 'Storage'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        LOWER(gcp_sku_description) LIKE '%coldline storage%'
      ) THEN 'Storage'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        LOWER(gcp_sku_description) LIKE '%archive storage%'
      ) THEN 'Storage'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        LOWER(gcp_sku_description) LIKE '%nearline storage%'
      ) THEN 'Storage'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'cloud storage' AND (LOWER(gcp_sku_description) LIKE '%operations%') THEN 'Storage'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'compute engine' AND LOWER(gcp_sku_description) LIKE '%pd capacity%' THEN 'Storage'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'compute engine' AND LOWER(gcp_sku_description) LIKE '%pd snapshot%' THEN 'Storage'
    WHEN
      (
        LOWER(
          gcp_service_description
        ) LIKE 'bigquery' AND LOWER(gcp_sku_description) LIKE '%storage%'
      ) THEN 'Storage'
    WHEN
      (
        LOWER(
          gcp_service_description
        ) LIKE 'cloud sql' AND LOWER(gcp_sku_description) LIKE '%storage%'
      ) THEN 'Storage'

    -- COMPUTE
    WHEN LOWER(gcp_sku_description) LIKE '%commitment%' THEN 'Committed Usage'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'bigquery' AND LOWER(gcp_sku_description) NOT LIKE 'storage' THEN 'Compute'
    WHEN
      LOWER(gcp_service_description) LIKE 'cloud sql' AND (LOWER(gcp_sku_description) LIKE '%cpu%'
        OR LOWER(gcp_sku_description) LIKE '%ram%') THEN 'Compute'
    WHEN LOWER(gcp_service_description) LIKE 'kubernetes engine' THEN 'Compute'
    WHEN LOWER(gcp_service_description) LIKE 'vertex ai' THEN 'Compute'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'compute engine' AND LOWER(gcp_sku_description) LIKE '%gpu%' THEN 'Compute'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%memorystore%' AND LOWER(gcp_sku_description) LIKE '%capacity%' THEN 'Compute'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'compute engine' AND (LOWER(gcp_sku_description) LIKE '%ram%'
        OR LOWER(gcp_sku_description) LIKE '%cpu%'
        OR LOWER(gcp_sku_description) LIKE '%core%') THEN 'Compute'

    -- NETWORKING
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%cloud sql%' AND LOWER(gcp_sku_description) LIKE '%networking%' THEN 'Networking'
    WHEN LOWER(gcp_service_description) LIKE '%cloud pub/sub%' THEN 'Networking'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%memorystore%' AND LOWER(gcp_sku_description) LIKE '%networking%' THEN 'Networking'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%cloud storage%' AND LOWER(gcp_sku_description) LIKE '%egress%' THEN 'Networking'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%cloud storage%' AND LOWER(gcp_sku_description) LIKE '%cdn%' THEN 'Networking'
    WHEN LOWER(gcp_service_description) = 'networking' THEN 'Networking'
    WHEN LOWER(gcp_sku_description) LIKE '%load balanc%' THEN 'Networking'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%compute engine%' AND LOWER(gcp_sku_description) LIKE '%ip%' THEN 'Networking'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%compute engine%' AND LOWER(gcp_sku_description) LIKE '%network%' THEN 'Networking'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%cloud storage%' AND (LOWER(gcp_sku_description) LIKE '%network%'
        OR LOWER(gcp_sku_description) LIKE '%download%') THEN 'Networking'


    -- LOGGING AND METRICS
    WHEN LOWER(gcp_service_description) LIKE 'stackdriver monitoring' THEN 'Logging and Metrics'
    WHEN LOWER(gcp_service_description) LIKE 'cloud logging' THEN 'Logging and Metrics'

    -- SUPPORT
    WHEN LOWER(gcp_sku_description) LIKE '%support%' THEN 'Support'
    WHEN LOWER(gcp_sku_description) LIKE '%security command center%' THEN 'Support'
    WHEN LOWER(gcp_sku_description) LIKE '%marketplace%' THEN 'Support'
    ELSE 'Other'
  END                                       AS finance_sku_type,
  CASE
    -- STORAGE
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        LOWER(gcp_sku_description) LIKE '%standard storage%'
      ) THEN 'Object (storage)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        LOWER(gcp_sku_description) LIKE '%coldline storage%'
      ) THEN 'Object (storage)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        LOWER(gcp_sku_description) LIKE '%archive storage%'
      ) THEN 'Object (storage)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        LOWER(gcp_sku_description) LIKE '%nearline storage%'
      ) THEN 'Object (storage)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        LOWER(gcp_sku_description) LIKE '%operations%'
      ) THEN 'Object (operations)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'compute engine' AND LOWER(
        gcp_sku_description
      ) LIKE '%pd capacity%' THEN 'Repository (storage)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'compute engine' AND LOWER(
        gcp_sku_description
      ) LIKE '%pd snapshot%' THEN 'Repository (storage)'
    WHEN
      (
        LOWER(
          gcp_service_description
        ) LIKE 'bigquery' AND LOWER(gcp_sku_description) LIKE '%storage%'
      ) THEN 'Data Warehouse (storage)'
    WHEN
      (
        LOWER(
          gcp_service_description
        ) LIKE 'cloud sql' AND LOWER(gcp_sku_description) LIKE '%storage%'
      ) THEN 'Databases (storage)'

    -- COMPUTE
    WHEN LOWER(gcp_sku_description) LIKE '%commitment%' THEN 'Committed Usage'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'bigquery' AND LOWER(
        gcp_sku_description
      ) NOT LIKE 'storage' THEN 'Data Warehouse (compute)'
    WHEN
      LOWER(gcp_service_description) LIKE 'cloud sql' AND (LOWER(gcp_sku_description) LIKE '%cpu%'
        OR LOWER(gcp_sku_description) LIKE '%ram%') THEN 'Data Warehouse (compute)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'kubernetes engine' THEN 'Container orchestration (compute)'
    WHEN LOWER(gcp_service_description) LIKE 'vertex ai' THEN 'AI/ML (compute)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'compute engine' AND LOWER(gcp_sku_description) LIKE '%gpu%' THEN 'AI/ML (compute)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%memorystore%' AND LOWER(
        gcp_sku_description
      ) LIKE '%capacity%' THEN 'Memorystores (compute)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'compute engine' AND (LOWER(gcp_sku_description) LIKE '%ram%'
        OR LOWER(gcp_sku_description) LIKE '%cpu%'
        OR LOWER(gcp_sku_description) LIKE '%core%') THEN 'Containers (compute)'

    -- NETWORKING
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%cloud sql%' AND LOWER(
        gcp_sku_description
      ) LIKE '%networking%' THEN 'Databases (networking)'
    WHEN LOWER(gcp_service_description) LIKE '%cloud pub/sub%' THEN 'Messaging (networking)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%memorystore%' AND LOWER(
        gcp_sku_description
      ) LIKE '%networking%' THEN 'Memorystores (networking)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%cloud storage%' AND LOWER(
        gcp_sku_description
      ) LIKE '%egress%' THEN 'Object (networking)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%cloud storage%' AND LOWER(
        gcp_sku_description
      ) LIKE '%cdn%' THEN 'Object CDN (networking)'
    WHEN LOWER(gcp_service_description) = 'networking' THEN 'Networking (mixed)'
    WHEN LOWER(gcp_sku_description) LIKE '%load balanc%' THEN 'Load Balancing (networking)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%compute engine%' AND LOWER(
        gcp_sku_description
      ) LIKE '%ip%' THEN 'Container networking (networking)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%compute engine%' AND LOWER(
        gcp_sku_description
      ) LIKE '%network%' THEN 'Container networking (networking)'
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE '%cloud storage%' AND (LOWER(gcp_sku_description) LIKE '%network%'
        OR LOWER(gcp_sku_description) LIKE '%download%') THEN 'Container networking (networking)'


    -- LOGGING AND METRICS
    WHEN
      LOWER(
        gcp_service_description
      ) LIKE 'stackdriver monitoring' THEN 'Metrics (logging and metrics)'
    WHEN LOWER(gcp_service_description) LIKE 'cloud logging' THEN 'Logging (logging and metrics)'

    -- SUPPORT
    WHEN LOWER(gcp_sku_description) LIKE '%support%' THEN 'Google Support (support)'
    WHEN LOWER(gcp_sku_description) LIKE '%security command center%' THEN 'Security (support)'
    WHEN LOWER(gcp_sku_description) LIKE '%marketplace%' THEN 'Marketplace (support)'
    ELSE 'Other'
  END                                       AS finance_sku_subtype,
  export.usage_unit                         AS usage_unit,
  export.pricing_unit                       AS pricing_unit,
  -- usage amount
  SUM(export.usage_amount)                  AS usage_amount,
  -- usage amount in p unit
  SUM(export.usage_amount_in_pricing_units) AS usage_amount_in_pricing_units,
  SUM(export.cost_before_credits)           AS cost_before_credits,
  SUM(export.total_cost)                    AS net_cost
FROM export
{{ dbt_utils.group_by(n=8) }}
