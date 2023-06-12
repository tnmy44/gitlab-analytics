{{ config({
    "materialized": "incremental",
    "unique_key": "dim_installation_id",
    })
}}

WITH umau_base AS (

    SELECT DISTINCT
      dim_installation_id,
      ping_delivery_type AS delivery_type,
      ping_created_date_month AS reporting_month, 
      ping_edition AS edition, 
      ping_product_tier AS product_tier,
      major_version, 
      minor_version, 
      major_minor_version,
      monthly_metric_value,
      CASE
        WHEN is_umau = TRUE 
          THEN 'umau'
        WHEN stage_name = 'verify'
          AND is_smau = TRUE 
            THEN 'verify_smau'
        WHEN stage_name = 'package'
          AND is_smau = TRUE 
            THEN 'package_smau'
        WHEN stage_name = 'deploy'
          AND is_smau = TRUE
            THEN 'deploy_smau'
        WHEN stage_name = 'monitor'
          AND is_smau = TRUE
            THEN 'monitor_smau'
       END AS pivot_metric
    FROM prod.common_mart.mart_ping_instance_metric_monthly
    WHERE pivot_metric IS NOT NULL
    
), pivot_base AS (

  SELECT 
    *
  FROM umau_base
    PIVOT(SUM(monthly_metric_value) FOR pivot_metric IN ('umau', 'verify_smau', 'package_smau','monitor_smau', 'deploy_smau'))
      AS p (dim_installation_id, delivery_type, reporting_month, edition, product_tier, major_version, minor_version, major_minor_version, umau, verify_smau, package_smau, monitor_smau, deploy_smau)
 )

SELECT 
  dim_installation_id,
  delivery_type,
  reporting_month,
  edition,
  product_tier,
  major_version,
  minor_version,
  ZEROIFNULL(umau) AS umau,
  ZEROIFNULL(verify_smau + package_smau + monitor_smau + deploy_smau) AS cmau,
  ZEROIFNULL(verify_smau) AS verify_smau,
  ZEROIFNULL(package_smau) AS package_smau,
  ZEROIFNULL(monitor_smau) AS monitor_smau,
  ZEROIFNULL(deploy_smau) AS deploy_smau
FROM pivot_base

{{ dbt_audit(
    created_by="@nhervas",
    updated_by="@nhervas",
    created_date="2023-06-12",
    updated_date="2023-06-12"
) }}