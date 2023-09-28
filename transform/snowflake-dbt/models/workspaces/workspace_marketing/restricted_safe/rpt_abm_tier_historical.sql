{{ config(materialized='table') }}

{{ simple_cte([
    ('sfdc_account_snapshots_source','sfdc_account_snapshots_source'),
    ('mart_crm_opportunity_stamped_hierarchy_hist','mart_crm_opportunity_stamped_hierarchy_hist'),
    ('rpt_lead_to_revenue','rpt_lead_to_revenue'), 
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
               
), opp_history_final AS (
  
  SELECT
  --IDs
    dim_crm_opportunity_id,
    dim_crm_account_id,
  
  --Opp Data  
    is_net_arr_closed_deal,
    is_net_arr_pipeline_created,
    is_sao,
    is_won,
   
  --Opp Dates
    created_date,
    sales_accepted_date,
    close_date
  FROM mart_crm_opportunity_stamped_hierarchy_hist
  WHERE created_date >= '2022-02-01'
    OR sales_accepted_date >= '2022-02-01'
    OR close_date >= '2022-02-01'
  
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
    mql_date_latest_pt
  FROM rpt_lead_to_revenue
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

), sao_base AS (
  
  SELECT
   --IDs
    opp_history_final.dim_crm_opportunity_id,
  
  --Opp Data  

    opp_history_final.is_sao,
    opp_history_final.sales_accepted_date,
    account_history_final.abm_tier_1_date,
    account_history_final.abm_tier_2_date,
    account_history_final.abm_tier,
    CASE 
      WHEN is_sao = TRUE
        AND sales_accepted_date BETWEEN valid_from AND valid_to
        THEN TRUE
      ELSE FALSE
    END AS is_abm_tier_sao  
  FROM opp_history_final
  LEFT JOIN account_history_final
    ON opp_history_final.dim_crm_account_id=account_history_final.dim_crm_account_id
  LEFT JOIN mart_crm_person_source
    ON opp_history_final.dim_crm_account_id=mart_crm_person_source.dim_crm_account_id
  WHERE abm_tier IS NOT NULL
  AND sales_accepted_date IS NOT NULL
  AND (abm_tier_1_date IS NOT NULL
    OR abm_tier_2_date IS NOT NULL)

), cw_base AS (
  
  SELECT
   --IDs
    opp_history_final.dim_crm_opportunity_id,
  
  --Opp Data  
    opp_history_final.close_date,
    account_history_final.abm_tier_1_date,
    account_history_final.abm_tier_2_date,
    account_history_final.abm_tier,
    CASE 
      WHEN is_won = TRUE
        AND close_date BETWEEN valid_from AND valid_to
        THEN TRUE
      ELSE FALSE
    END AS is_abm_tier_closed_won 
  FROM opp_history_final
  LEFT JOIN account_history_final
    ON opp_history_final.dim_crm_account_id=account_history_final.dim_crm_account_id
  LEFT JOIN mart_crm_person_source
    ON opp_history_final.dim_crm_account_id=mart_crm_person_source.dim_crm_account_id
  WHERE abm_tier IS NOT NULL
  AND close_date IS NOT NULL
  AND (abm_tier_1_date IS NOT NULL
    OR abm_tier_2_date IS NOT NULL)
  
), unioned AS (
  
  SELECT
  inquiry_base.dim_crm_person_id,
  NULL AS dim_crm_opportunity_id,
  is_abm_tier_inquiry,
  NULL AS is_abm_tier_mql,
  NULL AS is_abm_tier_sao,
  NULL AS is_abm_tier_closed_won
FROM inquiry_base
UNION ALL
SELECT
  mql_base.dim_crm_person_id,
  NULL AS dim_crm_opportunity_id,
  NULL AS is_abm_tier_inquiry,
  is_abm_tier_mql,
  NULL AS is_abm_tier_sao,
  NULL AS is_abm_tier_closed_won
FROM mql_base
UNION ALL
SELECT
  NULL AS dim_crm_person_id,
  dim_crm_opportunity_id,
  NULL AS is_abm_tier_inquiry,
  NULL AS is_abm_tier_mql,
  is_abm_tier_sao,
  NULL AS is_abm_tier_closed_won
FROM sao_base
UNION ALL
SELECT
  NULL AS dim_crm_person_id,
  dim_crm_opportunity_id,
  NULL AS is_abm_tier_inquiry,
  NULL AS is_abm_tier_mql,
  NULL AS is_abm_tier_sao,
  is_abm_tier_closed_won
FROM cw_base
  
), final AS (

  SELECT
    dim_crm_person_id,
    dim_crm_opportunity_id,
    is_abm_tier_inquiry,
    is_abm_tier_mql,
    is_abm_tier_sao,
    is_abm_tier_closed_won
  FROM unioned
  WHERE
    is_abm_tier_inquiry = TRUE
    OR is_abm_tier_mql = TRUE
    OR is_abm_tier_sao = TRUE
    OR is_abm_tier_closed_won = TRUE

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-09-06",
    updated_date="2023-09-28",
  ) }}