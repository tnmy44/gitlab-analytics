{{ config(materialized='table') }}

{{ simple_cte([
    ('sfdc_account_snapshots_source','sfdc_account_snapshots_source'),
    ('dim_date','dim_date')
]) }}

, abm_tier_base AS (
  
SELECT DISTINCT
    account_id_18 AS dim_crm_account_id,
    ultimate_parent_id AS dim_crm_parent_account_id,
    abm_tier,
    dbt_valid_from,
    dbt_valid_to
  FROM sfdc_account_snapshots_source
  WHERE abm_tier IN ('Tier 1', 'Tier 2')
  
), abm_tier_final AS (
  
  SELECT
    dim_crm_parent_account_id,
    abm_tier,
    MIN(dbt_valid_from)::Date AS abm_tier_from,
    MAX(dbt_valid_to)::Date AS abm_tier_to
  FROM abm_tier_base
  GROUP BY 1,2
  
), final AS (

SELECT 
abm_tier_final.*,
abm_first_day_of_quarter.first_day_of_fiscal_quarter AS amb_tier_from_quarter,
DATE (abm_last_day_of_quarter.first_day_of_month-1) AS abm_tier_to_month_end
FROM abm_tier_final
LEFT JOIN dim_date abm_first_day_of_quarter 
  ON abm_tier_from::DATE = abm_first_day_of_quarter.date_day
LEFT JOIN dim_date abm_last_day_of_quarter 
  ON abm_tier_to::DATE = abm_last_day_of_quarter.date_day

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-09-07",
    updated_date="2023-09-07",
  ) }}