{{ config(materialized='view') }}

WITH final AS (

select
   YEARLY_NAME,
   YEARLY_DRI,
   YEARLY_DESCRIPTION,
   REPLACE(QUARTER_NAME, '_', '-') AS QUARTER,
   TO_DECIMAL(TARGET, 18, 2) as TARGETS_RAW
from
   {{ ref('sheetload_fy25_yearlies_target_source') }} UNPIVOT(TARGET for QUARTER_NAME in 
   (
      "FY25_Q4",
      "FY25_Q3",
      "FY25_Q2",
      "FY25_Q1"
   )
))

{{ dbt_audit(
    cte_ref="final",
    created_by="@jonglee1218",
    updated_by="@jonglee1218",
    created_date="2024-03-26",
    updated_date="2024-03-26"
) }}