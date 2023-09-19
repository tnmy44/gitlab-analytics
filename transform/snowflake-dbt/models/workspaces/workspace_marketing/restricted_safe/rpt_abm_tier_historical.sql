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
    dbt_valid_from,
    dbt_valid_to
  FROM sfdc_account_snapshots_source
  WHERE abm_tier_1_date >= '2022-02-01'
    OR abm_tier_2_date >= '2022-02-01'
  
), account_history_final AS (
  
  SELECT
    dim_crm_account_id,
    dim_crm_user_id,
    dim_crm_parent_account_id,
    abm_tier_1_date,
    abm_tier_2_date,
    abm_tier_3_date,
    abm_tier
  FROM account_history_source
                
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
    is_sao,
    is_won,
   
  --Opp Dates
    sales_accepted_date,
    close_date
  FROM mart_crm_opportunity_daily_snapshot
  WHERE sales_accepted_date >= '2022-02-01'
    OR close_date >= '2022-02-01'
  
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
    mql_date_latest_pt,

  --Attributed Metrics
    inquiry_sum,
    mql_sum,
    influenced_opportunity_id
  FROM rpt_lead_to_revenue
 
), inquiry_base AS (
  
  SELECT
  --IDs
    mart_crm_person_source.dim_crm_person_id,
    mart_crm_person_source.dim_crm_account_id,
    mart_crm_person_source.sfdc_record_id,
    account_history_final.dim_crm_parent_account_id,

  --Person Data
    mart_crm_person_source.email_hash,
    mart_crm_person_source.inquiry_sum,
    mart_crm_person_source.is_inquiry,
    mart_crm_person_source.true_inquiry_date,
    mart_crm_person_source.status,
    account_history_final.abm_tier_1_date,
    account_history_final.abm_tier_2_date,
    CASE 
      WHEN true_inquiry_date IS NOT NULL
        THEN email_hash
      ELSE NULL
    END AS inquiry
  FROM mart_crm_person_source
  LEFT JOIN account_history_final
    ON mart_crm_person_source.dim_crm_account_id=account_history_final.dim_crm_account_id
  WHERE true_inquiry_date IS NOT NULL
  AND (abm_tier_1_date IS NOT NULL
    OR abm_tier_2_date IS NOT NULL)

), inquiry_final AS (

  SELECT
  --IDs
    dim_crm_account_id,
    dim_crm_parent_account_id,

  --Dates
    true_inquiry_date,
    abm_tier_1_date,
    abm_tier_2_date,

  --KPIs
    COUNT(DISTINCT inquiry) AS total_inquiries,
    SUM(inquiry_sum) AS attributed_inquiries
  FROM inquiry_base
  {{dbt_utils.group_by(n=5)}}
  
), mql_base AS (
  
  SELECT
  --IDs
    mart_crm_person_source.dim_crm_person_id,
    mart_crm_person_source.dim_crm_account_id,
    mart_crm_person_source.sfdc_record_id,
    account_history_final.dim_crm_parent_account_id,
  
  --Person Data
    mart_crm_person_source.email_hash,
    mart_crm_person_source.is_mql,
    mart_crm_person_source.status,
    mart_crm_person_source.mql_sum,
    CASE 
      WHEN is_mql = TRUE THEN email_hash
      ELSE NULL
    END AS mqls,
    mart_crm_person_source.mql_date_latest_pt,
    account_history_final.abm_tier_1_date,
    account_history_final.abm_tier_2_date    
  FROM mart_crm_person_source
  LEFT JOIN account_history_final
    ON mart_crm_person_source.dim_crm_account_id=account_history_final.dim_crm_account_id
  WHERE mql_date_latest_pt IS NOT NULL
  AND (abm_tier_1_date IS NOT NULL
    OR abm_tier_2_date IS NOT NULL)
  
), mql_final AS (

  SELECT
  --IDs
    dim_crm_account_id,
    dim_crm_parent_account_id,

  --Dates
    mql_date_latest_pt,
    abm_tier_1_date,
    abm_tier_2_date,

  --KPIs
    COUNT(DISTINCT mqls) AS total_mqls,
    SUM(mql_sum) AS attributed_mqls
  FROM mql_base
  {{dbt_utils.group_by(n=5)}}

), sao_base AS (
  
  SELECT
   --IDs
    opp_history_final.dim_crm_opportunity_id,
    opp_history_final.dim_crm_account_id,
    account_history_final.dim_crm_parent_account_id,
  
  --Opp Data  
    opp_history_final.order_type,
    opp_history_final.sales_qualified_source_name,
    opp_history_final.net_arr,
    opp_history_final.is_sao,
    opp_history_final.is_net_arr_closed_deal,
    opp_history_final.is_net_arr_pipeline_created,
    opp_history_final.sales_accepted_date,
    CASE 
      WHEN is_sao = TRUE THEN opp_history_final.dim_crm_opportunity_id 
      ELSE NULL 
    END AS saos,
    CASE 
      WHEN is_net_arr_pipeline_created = TRUE THEN opp_history_final.net_arr 
      ELSE NULL 
    END AS sao_net_arr,
    CASE 
      WHEN is_sao = TRUE THEN mart_crm_person_source.influenced_opportunity_id 
      ELSE NULL 
    END AS influenced_saos,
    account_history_final.abm_tier_1_date,
    account_history_final.abm_tier_2_date 
  FROM opp_history_final
  LEFT JOIN account_history_final
    ON opp_history_final.dim_crm_account_id=account_history_final.dim_crm_account_id
  LEFT JOIN mart_crm_person_source
    ON opp_history_final.dim_crm_account_id=mart_crm_person_source.dim_crm_account_id
  WHERE abm_tier IS NOT NULL
  AND sales_accepted_date IS NOT NULL
  AND (abm_tier_1_date IS NOT NULL
    OR abm_tier_2_date IS NOT NULL)

), sao_final AS (

  SELECT
  --IDs
    dim_crm_account_id,
    dim_crm_parent_account_id,

  --Opp Data
    order_type,
    sales_qualified_source_name,

  --Dates
    sales_accepted_date,
    abm_tier_1_date,
    abm_tier_2_date,

  --KPIs
    COUNT(DISTINCT saos) AS total_saos,
    SUM(sao_net_arr) AS total_sao_net_arr,
    COUNT(DISTINCT influenced_saos) AS attributed_saos
  FROM sao_base
  {{dbt_utils.group_by(n=7)}}

), cw_base AS (
  
  SELECT
   --IDs
    opp_history_final.dim_crm_opportunity_id,
    opp_history_final.dim_crm_account_id,
    account_history_final.dim_crm_parent_account_id,
  
  --Opp Data  
    opp_history_final.order_type,
    opp_history_final.sales_qualified_source_name,
    opp_history_final.net_arr,
    opp_history_final.is_won,
    opp_history_final.is_net_arr_closed_deal,
    opp_history_final.is_net_arr_pipeline_created,
    opp_history_final.close_date,
    CASE 
      WHEN is_won = TRUE THEN opp_history_final.dim_crm_opportunity_id 
      ELSE NULL 
    END AS cw,
    CASE 
      WHEN is_net_arr_closed_deal = TRUE THEN opp_history_final.net_arr 
      ELSE NULL 
    END AS cw_net_arr,
    CASE 
      WHEN is_won = TRUE THEN mart_crm_person_source.influenced_opportunity_id 
      ELSE NULL 
    END AS influenced_cw,
    account_history_final.abm_tier_1_date,
    account_history_final.abm_tier_2_date 
  FROM opp_history_final
  LEFT JOIN account_history_final
    ON opp_history_final.dim_crm_account_id=account_history_final.dim_crm_account_id
  LEFT JOIN mart_crm_person_source
    ON opp_history_final.dim_crm_account_id=mart_crm_person_source.dim_crm_account_id
  WHERE abm_tier IS NOT NULL
  AND close_date IS NOT NULL
  AND (abm_tier_1_date IS NOT NULL
    OR abm_tier_2_date IS NOT NULL)

), cw_final AS (

  SELECT
  --IDs
    dim_crm_account_id,
    dim_crm_parent_account_id,

  --Opp Data
    order_type,
    sales_qualified_source_name,

  --Dates
    close_date,
    abm_tier_1_date,
    abm_tier_2_date,

  --KPIs
    COUNT(DISTINCT cw) AS total_cw,
    SUM(cw_net_arr) AS total_cw_net_arr,
    COUNT(DISTINCT influenced_cw) AS attributed_cw
  FROM cw_base
  {{dbt_utils.group_by(n=7)}}
  
), final AS (
  
  SELECT
  -- Name
    'Inquiry' AS kpi_name,

  -- Dates
    abm_tier_1_date,
    abm_tier_2_date,
    true_inquiry_date AS kpi_date,
  
  -- Opp Data
    NULL AS order_type,
    NULL AS sales_qualified_source_name,
    NULL AS net_arr,

    SUM(total_inquiries) AS kpi,
    SUM(attributed_inquiries) AS attributed_kpi
  FROM inquiry_final
  {{dbt_utils.group_by(n=7)}}
  UNION ALL
  SELECT
  -- Name
    'MQLs' AS kpi_name,
  
  -- Dates
    abm_tier_1_date,
    abm_tier_2_date,
    mql_date_latest_pt AS kpi_date,
  
  -- Opp Data
    NULL AS order_type,
    NULL AS sales_qualified_source_name,
    NULL AS net_arr,

    SUM(total_mqls) AS kpi,
    SUM(attributed_mqls) AS attributed_kpi
  FROM mql_final
  {{dbt_utils.group_by(n=7)}}
  UNION ALL
  SELECT
  -- Name
    'SAOs' AS kpi_name,

  -- Dates
    abm_tier_1_date,
    abm_tier_2_date,
    sales_accepted_date AS kpi_date,
  
  -- Opp Data
    order_type,
    sales_qualified_source_name,
    SUM(total_sao_net_arr) AS net_arr,

    SUM(total_saos) AS kpi,
    SUM(attributed_saos) AS attributed_kpi
  FROM sao_final
  {{dbt_utils.group_by(n=6)}}
  UNION ALL
  SELECT
  -- Name
    'Closed Won' AS kpi_name, 
  
  -- Dates
    abm_tier_1_date,
    abm_tier_2_date,
    close_date AS kpi_date,
  
  -- Opp Data
    order_type,
    sales_qualified_source_name,
    SUM(total_cw_net_arr) AS net_arr,

    SUM(total_cw) AS kpi,
    SUM(attributed_cw) AS attributed_kpi
  FROM cw_final 
  {{dbt_utils.group_by(n=6)}}
  
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-09-06",
    updated_date="2023-09-18",
  ) }}