{{ config(
    materialized='table',
    )
}}


WITH service_base AS (

  SELECT * FROM {{ ref('rpt_gcp_billing_pl_day_combined') }}


),

lookback_pl_mappings AS (

  SELECT * FROM {{ ref('lookback_pl_mappings') }}

),

overlaps AS (

  SELECT
    service_base.date_day                                                                    AS date_day,
    service_base.gcp_project_id                                                              AS gcp_project_id,
    service_base.gcp_service_description,
    service_base.gcp_sku_description,
    service_base.infra_label,
    service_base.env_label,
    service_base.runner_label,
    service_base.folder_label,
    lookback_pl_mappings.pl_category,
    service_base.usage_unit,
    service_base.pricing_unit,
    service_base.usage_amount * COALESCE(lookback_pl_mappings.pl_percent, 1)                  AS usage_amount,
    service_base.usage_amount_in_pricing_units * COALESCE(lookback_pl_mappings.pl_percent, 1) AS usage_amount_in_pricing_units,
    service_base.cost_before_credits * COALESCE(lookback_pl_mappings.pl_percent, 1)           AS cost_before_credits,
    service_base.net_cost * COALESCE(lookback_pl_mappings.pl_percent, 1)                      AS net_cost,
    service_base.usage_standard_unit,
    service_base.usage_amount_in_standard_unit * COALESCE(lookback_pl_mappings.pl_percent, 1) AS usage_amount_in_standard_unit,
    COALESCE(lookback_pl_mappings.from_mapping, service_base.from_mapping)                    AS from_mapping,
    DENSE_RANK() OVER (PARTITION BY service_base.date_day,
      service_base.gcp_project_id,
      service_base.gcp_service_description,
      service_base.gcp_sku_description,
      service_base.infra_label,
      service_base.env_label,
      service_base.runner_label,
      service_base.folder_label
      ORDER BY
        (CASE WHEN lookback_pl_mappings.gcp_service_description IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.gcp_sku_description IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.infra_label IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.env_label IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.runner_label IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.folder_label IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.gcp_project_id IS NOT NULL THEN 1 ELSE 0 END) DESC
    )                                                                                        AS priority
  FROM
    service_base
  LEFT JOIN lookback_pl_mappings ON lookback_pl_mappings.date_day = service_base.date_day
    AND COALESCE(service_base.gcp_project_id, 'null') like COALESCE(lookback_pl_mappings.gcp_project_id, COALESCE(service_base.gcp_project_id, ''))
    AND COALESCE(lookback_pl_mappings.gcp_service_description, service_base.gcp_service_description) = service_base.gcp_service_description
    AND COALESCE(lookback_pl_mappings.gcp_sku_description, service_base.gcp_sku_description) = service_base.gcp_sku_description
    AND COALESCE(lookback_pl_mappings.infra_label, COALESCE(service_base.infra_label, '')) = COALESCE(service_base.infra_label, '')
    AND COALESCE(lookback_pl_mappings.env_label, COALESCE(service_base.env_label, '')) = COALESCE(service_base.env_label, '')
    AND COALESCE(lookback_pl_mappings.runner_label, COALESCE(service_base.runner_label, '')) = COALESCE(service_base.runner_label, '')
    AND COALESCE(lookback_pl_mappings.folder_label, COALESCE(service_base.folder_label, 0)) = COALESCE(service_base.folder_label, 0)

)

SELECT *
  EXCLUDE(priority)
FROM overlaps
WHERE priority = 1
