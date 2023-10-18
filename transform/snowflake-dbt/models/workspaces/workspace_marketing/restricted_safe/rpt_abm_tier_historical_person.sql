{{ config(materialized='table') }}

{{ simple_cte([
    ('sfdc_account_snapshots_source','sfdc_account_snapshots_source'),
    ('fct_crm_person','fct_crm_person'), 
    ('dim_date','dim_date')
]) }}

, account_history_final AS (
 
  SELECT
    account_id_18 AS dim_crm_account_id,
    owner_id AS dim_crm_user_id,
    ultimate_parent_id AS dim_crm_parent_account_id,
    abm_tier_1_date,
    abm_tier_2_date,
    abm_tier,
    MIN(dbt_valid_from)::DATE AS valid_from,
    MAX(dbt_valid_to)::DATE AS valid_to
  FROM sfdc_account_snapshots_source
  WHERE abm_tier_1_date >= '2022-02-01'
    OR abm_tier_2_date >= '2022-02-01'
  {{dbt_utils.group_by(n=6)}}
  
), mart_crm_person_source AS (

  SELECT
  --IDs
    dim_crm_person_id,
    dim_crm_account_id,
  
  --Person Data
    is_mql,
    is_inquiry,
    
  --Person Dates
    true_inquiry_date,
    mql_datetime_latest_pt::DATE AS mql_date_latest_pt
  FROM fct_crm_person
  WHERE true_inquiry_date >= '2022-02-01'
    OR mql_date_latest_pt >= '2022-02-01'
 
), inquiry_base AS (
  
  SELECT
  --IDs
    mart_crm_person_source.dim_crm_person_id,

  --Person Data
    mart_crm_person_source.true_inquiry_date,
    account_history_final.abm_tier_1_date,
    account_history_final.abm_tier_2_date,
    account_history_final.abm_tier,
    CASE 
      WHEN true_inquiry_date IS NOT NULL
        AND true_inquiry_date BETWEEN valid_from AND valid_to
        THEN TRUE
      ELSE FALSE
    END AS is_abm_tier_inquiry
  FROM mart_crm_person_source
  LEFT JOIN account_history_final
    ON mart_crm_person_source.dim_crm_account_id=account_history_final.dim_crm_account_id
  WHERE abm_tier IS NOT NULL
  AND true_inquiry_date IS NOT NULL
  AND (abm_tier_1_date IS NOT NULL
    OR abm_tier_2_date IS NOT NULL)
  
), mql_base AS (
  
  SELECT
  --IDs
    mart_crm_person_source.dim_crm_person_id,
  
  --Person Data
    mart_crm_person_source.mql_date_latest_pt,
    account_history_final.abm_tier_1_date,
    account_history_final.abm_tier_2_date  ,
    account_history_final.abm_tier,
    CASE 
      WHEN mql_date_latest_pt IS NOT NULL
        AND mql_date_latest_pt BETWEEN valid_from AND valid_to
        THEN TRUE
      ELSE FALSE
    END AS is_abm_tier_mql  
  FROM mart_crm_person_source
  LEFT JOIN account_history_final
    ON mart_crm_person_source.dim_crm_account_id=account_history_final.dim_crm_account_id
  WHERE abm_tier IS NOT NULL
  AND mql_date_latest_pt IS NOT NULL
  AND (abm_tier_1_date IS NOT NULL
    OR abm_tier_2_date IS NOT NULL)
  
), unioned AS (
  
  SELECT
  inquiry_base.dim_crm_person_id,
  is_abm_tier_inquiry,
  NULL AS is_abm_tier_mql
FROM inquiry_base
UNION ALL
SELECT
  mql_base.dim_crm_person_id,
  NULL AS is_abm_tier_inquiry,
  is_abm_tier_mql
FROM mql_base
  
), final AS (

  SELECT
    dim_crm_person_id,
    is_abm_tier_inquiry,
    is_abm_tier_mql
  FROM unioned
  WHERE
    is_abm_tier_inquiry = TRUE
    OR is_abm_tier_mql = TRUE

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-09-06",
    updated_date="2023-10-18",
  ) }}