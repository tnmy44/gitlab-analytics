{{ config(
    materialized='table',
    tags=["mnpi_exception"]
    ) 
}}

SELECT
  yearly_name,
  yearly_dri,
  yearly_description,
  quarter,
  IFF(is_mnpi = true AND targets_raw IS NOT NULL, 1, targets_raw) AS targets_abstracted,
  CASE WHEN is_mnpi = true AND targets_raw IS NULL THEN NULL
       WHEN is_mnpi = true AND targets_raw IS NOT NULL THEN target_attainment
       ELSE actuals_raw END AS actuals_abstracted
FROM
  {{ ref('wk_yearlies_target_attainment_mnpi') }} AS yearlies_mnpi