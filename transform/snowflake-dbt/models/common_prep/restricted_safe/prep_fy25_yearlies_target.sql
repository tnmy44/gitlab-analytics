{{ config(materialized='view') }}

WITH final AS (

SELECT
   yearly_name,
   yearly_dri,
   yearly_description,
   REPLACE(quarter_name, '_', '-') AS quarter,
   TO_DECIMAL(target, 18, 2) AS targets_raw
FROM
   {{ ref('sheetload_fy25_yearlies_target_source') }} UNPIVOT(target for quarter_name in 
   (
      "FY25_Q4",
      "FY25_Q3",
      "FY25_Q2",
      "FY25_Q1"
   )
))

SELECT * 
FROM final