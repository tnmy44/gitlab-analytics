{{ config(materialized='view') }}

WITH prep as (
  
 SELECT 
  yearly_name,
  yearly_dri,
  yearly_description,
  is_mnpi,
  ifnull(FY25_Q4,-1) as FY25_Q4,
  ifnull(FY25_Q3,-1) as FY25_Q3,
  ifnull(FY25_Q2,-1) as FY25_Q2,
  ifnull(FY25_Q1,-1) as FY25_Q1

FROM
  {{ ref('sheetload_fy25_yearlies_target_source') }}
),
final AS (

SELECT
   yearly_name,
   yearly_dri,
   yearly_description,
   is_mnpi,
   REPLACE(quarter_name, '_', '-') AS quarter,
   case when target = -1 then null else target end AS targets_raw
FROM
   prep UNPIVOT(target FOR quarter_name IN 
   (
      "FY25_Q4",
      "FY25_Q3",
      "FY25_Q2",
      "FY25_Q1"
   )
))

SELECT * 
FROM final