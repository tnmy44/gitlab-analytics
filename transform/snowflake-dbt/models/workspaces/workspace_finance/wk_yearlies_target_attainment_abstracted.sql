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
  IFF(is_mnpi = true, 1, targets_raw) as targets_abstracted,
  IFF(is_mnpi = true, target_attainment, actuals_raw) as actuals_abstracted
FROM
  {{ ref('wk_yearlies_target_attainment_mnpi') }} AS yearlies_mnpi