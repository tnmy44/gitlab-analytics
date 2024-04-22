{{ config(materialized='table') }}

SELECT
  yearly_name,
  yearly_dri,
  yearly_description,
  quarter,
  case when is_mnpi = true then 1 else targets_raw end as targets_abstracted,
  case when is_mnpi = true then target_attainment else actuals_raw end as actuals_abstracted
FROM
  {{ ref('wk_yearlies_target_attainment_mnpi') }} AS yearlies_mnpi