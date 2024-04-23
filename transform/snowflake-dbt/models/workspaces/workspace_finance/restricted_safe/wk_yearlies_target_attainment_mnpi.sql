{{ config(materialized='table') }}

SELECT
  yearlies_target.yearly_name,
  yearlies_target.yearly_dri,
  yearlies_target.yearly_description,
  yearlies_target.is_mnpi,
  yearlies_target.quarter,
  yearlies_target.targets_raw,
  yearlies_actual.actuals_raw,
  DIV0NULL(yearlies_actual.actuals_raw,yearlies_target.targets_raw) AS target_attainment
FROM
  {{ ref('prep_yearlies_target') }} AS yearlies_target
  LEFT JOIN {{ ref('wk_prep_yearlies_actual') }} AS yearlies_actual
  ON yearlies_target.quarter = yearlies_actual.quarter
  AND yearlies_target.yearly_name = yearlies_actual.yearly_name
  