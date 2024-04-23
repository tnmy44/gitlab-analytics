{{ config(
    materialized='table',
    tags=["mnpi_exception"]
    ) 
}}

WITH final as (

SELECT
  yearly_name,
  yearly_dri,
  yearly_description,
  quarter,
  source_table,
  targets_abstracted,
  actuals_abstracted
FROM
  {{ ref('wk_yearlies_target_attainment_mnpi') }} AS yearlies_mnpi
)

SELECT
  *
FROM final