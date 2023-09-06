{{ config(materialized='table') }}

{{ simple_cte([
    ('sfdc_account_snapshots_source','sfdc_account_snapshots_source'),
    ('mart_crm_opportunity_daily_snapshot','mart_crm_opportunity_daily_snapshot'),
    ('rpt_lead_to_revenue','rpt_lead_to_revenue'), 
    ('dim_date','dim_date')
]) }}

, account_history_source AS (
 
  SELECT DISTINCT
    account_id_18 AS dim_crm_account_id,
    owner_id AS dim_crm_user_id,
    ultimate_parent_id AS dim_crm_parent_account_id,
    abm_tier_1_date,
    abm_tier_2_date,
    abm_tier_3_date,
    abm_tier,
    dbt_valid_from
  FROM sfdc_account_snapshots_source
  
), dim_date_base AS (
  
  SELECT 
    date_day,
    first_day_of_week,
    first_day_of_month,
    fiscal_quarter_name_fy,
    fiscal_year
  FROM dim_date
  -- WHERE day_of_fiscal_quarter = 90
  
), account_history_final AS (
  
  SELECT
    dim_crm_account_id,
    dim_crm_user_id,
    dim_crm_parent_account_id,
    abm_tier_1_date,
    abm_tier_2_date,
    abm_tier_3_date,
    abm_tier_1.fiscal_quarter_name_fy AS abm_tier_1_quarter,
    abm_tier_2.fiscal_quarter_name_fy AS abm_tier_2_quarter,
    abm_tier
  FROM account_history_source
  LEFT JOIN dim_date_base abm_tier_1
    ON account_history_source.abm_tier_1_date::Date=abm_tier_1.date_day
  LEFT JOIN dim_date_base abm_tier_2
    ON account_history_source.abm_tier_2_date::Date=abm_tier_2.date_day
  
), opp_history_final AS (
  
  SELECT
  --IDs
    dim_crm_opportunity_id,
    dim_crm_account_id,
  
  --Opp Data  
    order_type,
    sales_qualified_source_name,
    net_arr,
    is_net_arr_closed_deal,
    is_net_arr_pipeline_created,
   
  --Opp Dates
    sales_accepted_date,
    close_date
  FROM mart_crm_opportunity_daily_snapshot
  
), mart_crm_person_source AS (

  SELECT DISTINCT
  --IDs
    dim_crm_person_id,
    dim_crm_account_id,
    sfdc_record_id,
  
  --Person Data
    email_hash,
    is_mql,
    is_inquiry,
    status,
    
  --Person Dates
    true_inquiry_date,
    mql_date_latest_pt
  FROM rpt_lead_to_revenue
 
), inquiry_base AS (
  
  SELECT
  --IDs
    mart_crm_person_source.dim_crm_person_id,
    mart_crm_person_source.dim_crm_account_id,
    mart_crm_person_source.sfdc_record_id,
  
  --Person Data
    mart_crm_person_source.email_hash,
    mart_crm_person_source.is_mql,
    mart_crm_person_source.is_inquiry,
    mart_crm_person_source.status,
    dim_date_base.fiscal_quarter_name_fy AS inquiry_quarter,
    account_history_final.abm_tier_1_quarter,
    account_history_final.abm_tier_2_quarter
  FROM mart_crm_person_source
  LEFT JOIN dim_date_base
    ON mart_crm_person_source.true_inquiry_date=dim_date_base.date_day
  LEFT JOIN account_history_final
    ON inquiry_quarter=account_history_final.abm_tier_1_quarter
    OR inquiry_quarter=account_history_final.abm_tier_2_quarter 
  WHERE true_inquiry_date IS NOT NULL
  AND (abm_tier_1_quarter IS NOT NULL
    OR abm_tier_2_quarter IS NOT NULL)
  
), mql_base AS (
  
  SELECT
  --IDs
    mart_crm_person_source.dim_crm_person_id,
    mart_crm_person_source.dim_crm_account_id,
    mart_crm_person_source.sfdc_record_id,
  
  --Person Data
    mart_crm_person_source.email_hash,
    mart_crm_person_source.is_mql,
    mart_crm_person_source.is_inquiry,
    mart_crm_person_source.status,
    dim_date_base.fiscal_quarter_name_fy AS mql_quarter,
    account_history_final.abm_tier_1_quarter,
    account_history_final.abm_tier_2_quarter
  FROM mart_crm_person_source
  LEFT JOIN dim_date_base
    ON mart_crm_person_source.mql_date_latest_pt=dim_date_base.date_day
  LEFT JOIN account_history_final
    ON mql_quarter=account_history_final.abm_tier_1_quarter
    OR mql_quarter=account_history_final.abm_tier_2_quarter 
  WHERE mql_date_latest_pt IS NOT NULL
  AND (abm_tier_1_quarter IS NOT NULL
    OR abm_tier_2_quarter IS NOT NULL)
  
), sao_base AS (
  
  SELECT
   --IDs
    opp_history_source.dim_crm_opportunity_id,
    opp_history_source.dim_crm_account_id,
  
  --Opp Data  
    opp_history_source.order_type,
    opp_history_source.sales_qualified_source_name,
    opp_history_source.net_arr,
    opp_history_source.is_net_arr_closed_deal,
    opp_history_source.is_net_arr_pipeline_created,
    dim_date_base.fiscal_quarter_name_fy AS sao_quarter,
    account_history_final.abm_tier_1_quarter,
    account_history_final.abm_tier_2_quarter
  FROM opp_history_source
  LEFT JOIN dim_date_base
    ON opp_history_source.sales_accepted_date=dim_date_base.date_day
  LEFT JOIN account_history_final
    ON sao_quarter=account_history_final.abm_tier_1_quarter
    OR sao_quarter=account_history_final.abm_tier_2_quarter 
  WHERE abm_tier IS NOT null
  AND sales_accepted_date IS NOT NULL
  AND (abm_tier_1_quarter IS NOT NULL
    OR abm_tier_2_quarter IS NOT NULL)

), cw_base AS (
  
  SELECT
   --IDs
    opp_history_source.dim_crm_opportunity_id,
    opp_history_source.dim_crm_account_id,
  
  --Opp Data  
    opp_history_source.order_type,
    opp_history_source.sales_qualified_source_name,
    opp_history_source.net_arr,
    opp_history_source.is_net_arr_closed_deal,
    opp_history_source.is_net_arr_pipeline_created,
    dim_date_base.fiscal_quarter_name_fy AS cw_quarter,
    account_history_final.abm_tier_1_quarter,
    account_history_final.abm_tier_2_quarter
  FROM opp_history_source
  LEFT JOIN dim_date_base
    ON opp_history_source.close_date=dim_date_base.date_day
  LEFT JOIN account_history_final
    ON cw_quarter=account_history_final.abm_tier_1_quarter
    OR cw_quarter=account_history_final.abm_tier_2_quarter 
  WHERE abm_tier IS NOT null
  AND is_net_arr_closed_deal = TRUE
  AND (abm_tier_1_quarter IS NOT NULL
    OR abm_tier_2_quarter IS NOT NULL)
  
), final AS (
  
  SELECT
  --IDs
    'Inquiry' AS kpi_name,
    -- dim_crm_account_id,

  -- ABM Quarters
    abm_tier_1_quarter,
    abm_tier_2_quarter,
    inquiry_quarter AS kpi_quarter,
  
  -- Opp Data
    NULL AS order_type,
    NULL AS sales_qualified_source_name,
    NULL AS net_arr,

    COUNT(DISTINCT dim_crm_person_id) AS kpi
  FROM inquiry_base
  {{dbt_utils.group_by(n=7)}}
  UNION ALL
  SELECT
  --IDs
    'MQLs' AS kpi_name,
    -- dim_crm_account_id,
  
  -- ABM Quarters
    abm_tier_1_quarter,
    abm_tier_2_quarter,
    mql_quarter AS kpi_quarter,
  
  -- Opp Data
    NULL AS order_type,
    NULL AS sales_qualified_source_name,
    NULL AS net_arr,

    COUNT(DISTINCT dim_crm_person_id) AS kpi
  FROM mql_base
  {{dbt_utils.group_by(n=7)}}
  UNION ALL
  SELECT
  --IDs
    'SAOs' AS kpi_name,
    -- dim_crm_account_id,

  --Quarters
    abm_tier_1_quarter,
    abm_tier_2_quarter,
    sao_quarter AS kpi_quarter,
  
  -- Opp Data
    order_type,
    sales_qualified_source_name,
    net_arr,

    COUNT(DISTINCT dim_crm_opportunity_id) AS kpi
  FROM sao_base 
  {{dbt_utils.group_by(n=7)}}
  UNION ALL
  SELECT
  --IDs
    'Closed Won' AS kpi_name,
    -- dim_crm_account_id,  
  
  --Quarters
    abm_tier_1_quarter,
    abm_tier_2_quarter,
    cw_quarter AS kpi_quarter,
  
  -- Opp Data
    order_type,
    sales_qualified_source_name,
    net_arr,

    COUNT(DISTINCT dim_crm_opportunity_id) AS kpi
  FROM cw_base 
  {{dbt_utils.group_by(n=7)}}
  
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-09-06",
    updated_date="2023-09-06",
  ) }}