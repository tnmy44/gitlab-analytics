{{ config(materialized='table') }}

{{ simple_cte([
    ('person_base','mart_crm_person'),
    ('dim_crm_person','dim_crm_person'),
    ('mart_crm_opportunity_stamped_hierarchy_hist', 'mart_crm_opportunity_stamped_hierarchy_hist'), 
    ('mart_crm_touchpoint', 'mart_crm_touchpoint'),
    ('mart_crm_attribution_touchpoint','mart_crm_attribution_touchpoint'),
    ('dim_crm_account', 'dim_crm_account'),
    ('dim_date','dim_date')
]) }}
, upa_base AS ( --pulls every account and it's UPA
  
    SELECT 
      dim_parent_crm_account_id,
      dim_crm_account_id
    FROM dim_crm_account
), first_order_opps AS ( -- pulls only FO CW Opps and their UPA/Account ID

    SELECT
      dim_parent_crm_account_id,
      dim_crm_account_id,
      dim_crm_opportunity_id,
      close_date,
      is_sao,
      sales_accepted_date
    FROM mart_crm_opportunity_stamped_hierarchy_hist
    WHERE is_won = true
      AND order_type = '1. New - First Order'

), accounts_with_first_order_opps AS ( -- shows only UPA/Account with a FO Available Opp on it

    SELECT
      upa_base.dim_parent_crm_account_id,
      upa_base.dim_crm_account_id,
      first_order_opps.dim_crm_opportunity_id,
      FALSE AS is_first_order_available
    FROM upa_base 
    LEFT JOIN first_order_opps
      ON upa_base.dim_crm_account_id=first_order_opps.dim_crm_account_id
    WHERE dim_crm_opportunity_id IS NOT NULL

), person_order_type_base AS (

    SELECT DISTINCT
      person_base.email_hash, 
      person_base.dim_crm_account_id,
      upa_base.dim_parent_crm_account_id,
      mart_crm_opportunity_stamped_hierarchy_hist.dim_crm_opportunity_id,
      CASE 
         WHEN is_first_order_available = False AND mart_crm_opportunity_stamped_hierarchy_hist.order_type = '1. New - First Order' THEN '3. Growth'
         WHEN is_first_order_available = False AND mart_crm_opportunity_stamped_hierarchy_hist.order_type != '1. New - First Order' THEN mart_crm_opportunity_stamped_hierarchy_hist.order_type
      ELSE '1. New - First Order'
      END AS person_order_type,
      ROW_NUMBER() OVER( PARTITION BY email_hash ORDER BY person_order_type) AS person_order_type_number
    FROM person_base
    FULL JOIN upa_base
      ON person_base.dim_crm_account_id = upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN mart_crm_opportunity_stamped_hierarchy_hist
      ON upa_base.dim_parent_crm_account_id = mart_crm_opportunity_stamped_hierarchy_hist.dim_parent_crm_account_id

), person_order_type_final AS (

    SELECT DISTINCT
      email_hash,
      dim_crm_opportunity_id,
      dim_parent_crm_account_id,
      dim_crm_account_id,
      person_order_type
    FROM person_order_type_base
    WHERE person_order_type_number=1

), person_base_with_tp AS (

    SELECT DISTINCT
  --IDs
      person_base.dim_crm_person_id,
      person_base.dim_crm_account_id,
      dim_crm_account.dim_parent_crm_account_id,
      dim_crm_person.sfdc_record_id,
      mart_crm_touchpoint.dim_crm_touchpoint_id,
  
  --Person Data
      person_base.email_hash,
      person_base.email_domain_type,
      person_base.true_inquiry_date,
      person_base.mql_date_first_pt,
      person_base.status,
      person_base.lead_source,
      person_base.is_mql,
      person_base.account_demographics_sales_segment,
      person_base.account_demographics_region,
      person_base.account_demographics_geo,
      person_base.account_demographics_area,
      person_base.account_demographics_upa_country,
      person_base.account_demographics_territory,
      dim_crm_account.is_first_order_available,
      person_order_type_final.person_order_type,
  
  --Touchpoint Data
      'Person Touchpoint' AS touchpoint_type,
      mart_crm_touchpoint.bizible_touchpoint_date,
      mart_crm_touchpoint.bizible_touchpoint_position,
      mart_crm_touchpoint.bizible_touchpoint_source,
      mart_crm_touchpoint.bizible_touchpoint_type,
      mart_crm_touchpoint.bizible_ad_campaign_name,
      mart_crm_touchpoint.bizible_form_url,
      mart_crm_touchpoint.bizible_landing_page,
      mart_crm_touchpoint.bizible_form_url_raw,
      mart_crm_touchpoint.bizible_landing_page_raw,
      mart_crm_touchpoint.bizible_marketing_channel,
      mart_crm_touchpoint.bizible_marketing_channel_path,
      mart_crm_touchpoint.bizible_medium,
      mart_crm_touchpoint.bizible_referrer_page,
      mart_crm_touchpoint.bizible_referrer_page_raw,
      mart_crm_touchpoint.bizible_integrated_campaign_grouping,
      mart_crm_touchpoint.touchpoint_segment,
      mart_crm_touchpoint.gtm_motion,
      mart_crm_touchpoint.pipe_name,
      mart_crm_touchpoint.is_dg_influenced,
      mart_crm_touchpoint.is_dg_sourced,
      mart_crm_touchpoint.bizible_count_first_touch,
      mart_crm_touchpoint.bizible_count_lead_creation_touch,
      mart_crm_touchpoint.bizible_count_u_shaped,
      mart_crm_touchpoint.is_fmm_influenced,
      mart_crm_touchpoint.is_fmm_sourced,
      mart_crm_touchpoint.bizible_count_lead_creation_touch AS new_lead_created_sum,
      mart_crm_touchpoint.count_true_inquiry AS count_true_inquiry,
      mart_crm_touchpoint.count_inquiry AS inquiry_sum, 
      mart_crm_touchpoint.pre_mql_weight AS mql_sum,
      mart_crm_touchpoint.count_accepted AS accepted_sum,
      mart_crm_touchpoint.count_net_new_mql AS new_mql_sum,
      mart_crm_touchpoint.count_net_new_accepted AS new_accepted_sum   
    FROM person_base
    INNER JOIN dim_crm_person
      ON person_base.dim_crm_person_id = dim_crm_person.dim_crm_person_id
    LEFT JOIN upa_base
    ON person_base.dim_crm_account_id = upa_base.dim_crm_account_id
    LEFT JOIN dim_crm_account
    ON person_base.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    LEFT JOIN person_order_type_final
      ON person_base.email_hash = person_order_type_final.email_hash
    LEFT JOIN mart_crm_touchpoint
      ON mart_crm_touchpoint.email_hash = person_base.email_hash
  
  ), opp_base_with_batp AS (
    
    SELECT
    --IDs
      opp.dim_crm_opportunity_id,
      opp.dim_crm_account_id,
      dim_crm_account.dim_parent_crm_account_id,
      mart_crm_attribution_touchpoint.dim_crm_touchpoint_id,
    
    --Opp Data
      opp.order_type AS opp_order_type,
      opp.sales_qualified_source_name,
      opp.deal_path_name,
      opp.sales_type,
      opp.sales_accepted_date,
      opp.created_date AS opp_created_date,
      opp.close_date,
      opp.is_won,
      opp.is_sao,
      opp.new_logo_count,
      opp.net_arr,
      opp.is_net_arr_closed_deal,
      opp.crm_opp_owner_sales_segment_stamped,
      opp.crm_opp_owner_region_stamped,
      opp.crm_opp_owner_area_stamped,
      opp.crm_opp_owner_geo_stamped,
      opp.parent_crm_account_upa_country,
      opp.parent_crm_account_territory,
    
    -- Touchpoint Data
      'Attribution Touchpoint' AS touchpoint_type,
      mart_crm_attribution_touchpoint.bizible_touchpoint_date,
      mart_crm_attribution_touchpoint.bizible_touchpoint_position,
      mart_crm_attribution_touchpoint.bizible_touchpoint_source,
      mart_crm_attribution_touchpoint.bizible_touchpoint_type,
      mart_crm_attribution_touchpoint.bizible_ad_campaign_name,
      mart_crm_attribution_touchpoint.bizible_form_url,
      mart_crm_attribution_touchpoint.bizible_landing_page,
      mart_crm_attribution_touchpoint.bizible_form_url_raw,
      mart_crm_attribution_touchpoint.bizible_landing_page_raw,
      mart_crm_attribution_touchpoint.bizible_marketing_channel,
      mart_crm_attribution_touchpoint.bizible_marketing_channel_path,
      mart_crm_attribution_touchpoint.bizible_medium,
      mart_crm_attribution_touchpoint.bizible_referrer_page,
      mart_crm_attribution_touchpoint.bizible_referrer_page_raw,
      mart_crm_attribution_touchpoint.bizible_integrated_campaign_grouping,
      mart_crm_attribution_touchpoint.touchpoint_segment,
      mart_crm_attribution_touchpoint.gtm_motion,
      mart_crm_attribution_touchpoint.pipe_name,
      mart_crm_attribution_touchpoint.is_dg_influenced,
      mart_crm_attribution_touchpoint.is_dg_sourced,
      mart_crm_attribution_touchpoint.bizible_count_first_touch,
      mart_crm_attribution_touchpoint.bizible_count_lead_creation_touch,
      mart_crm_attribution_touchpoint.bizible_count_u_shaped,
      mart_crm_attribution_touchpoint.bizible_count_w_shaped,
      mart_crm_attribution_touchpoint.bizible_count_custom_model,
      mart_crm_attribution_touchpoint.bizible_weight_u_shaped,
      mart_crm_attribution_touchpoint.bizible_weight_w_shaped,
      mart_crm_attribution_touchpoint.bizible_weight_full_path,
      mart_crm_attribution_touchpoint.bizible_weight_custom_model,
      mart_crm_attribution_touchpoint.bizible_weight_first_touch,
      mart_crm_attribution_touchpoint.is_fmm_influenced,
      mart_crm_attribution_touchpoint.is_fmm_sourced,
      CASE
          WHEN mart_crm_attribution_touchpoint.dim_crm_touchpoint_id IS NOT null THEN opp.dim_crm_opportunity_id
          ELSE null
      END AS influenced_opportunity_id,
      SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) AS first_opp_created,
      SUM(mart_crm_attribution_touchpoint.bizible_count_u_shaped) AS u_shaped_opp_created,
      SUM(mart_crm_attribution_touchpoint.bizible_count_w_shaped) AS w_shaped_opp_created,
      SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) AS full_shaped_opp_created,
      SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) AS custom_opp_created,
      SUM(mart_crm_attribution_touchpoint.l_weight) AS linear_opp_created,
      SUM(mart_crm_attribution_touchpoint.first_net_arr) AS first_net_arr,
      SUM(mart_crm_attribution_touchpoint.u_net_arr) AS u_net_arr,
      SUM(mart_crm_attribution_touchpoint.w_net_arr) AS w_net_arr,
      SUM(mart_crm_attribution_touchpoint.full_net_arr) AS full_net_arr,
      SUM(mart_crm_attribution_touchpoint.custom_net_arr) AS custom_net_arr,
      SUM(mart_crm_attribution_touchpoint.linear_net_arr) AS linear_net_arr,
      CASE
        WHEN opp.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS first_sao,
      CASE 
        WHEN opp.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_u_shaped) 
        ELSE 0 
      END AS u_shaped_sao,
      CASE 
        WHEN opp.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_w_shaped) 
        ELSE 0 
      END AS w_shaped_sao,
      CASE 
        WHEN opp.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS full_shaped_sao,
      CASE 
        WHEN opp.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) 
        ELSE 0 
      END AS custom_sao,
      CASE 
        WHEN opp.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.l_weight) 
        ELSE 0 
      END AS linear_sao,
      CASE 
        WHEN opp.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.u_net_arr) 
        ELSE 0 
      END AS pipeline_first_net_arr,
      CASE 
        WHEN opp.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.u_net_arr) 
        ELSE 0 
        END AS pipeline_u_net_arr,
      CASE 
        WHEN opp.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.w_net_arr) 
        ELSE 0 
      END AS pipeline_w_net_arr,
      CASE 
        WHEN opp.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.full_net_arr) 
        ELSE 0 
      END AS pipeline_full_net_arr,
      CASE 
        WHEN opp.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.custom_net_arr) 
        ELSE 0 
      END AS pipeline_custom_net_arr,
      CASE 
        WHEN opp.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.linear_net_arr) 
        ELSE 0 
      END AS pipeline_linear_net_arr,
      CASE 
        WHEN opp.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS won_first,
      CASE 
        WHEN opp.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_u_shaped) 
        ELSE 0 
      END AS won_u,
      CASE 
        WHEN opp.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_w_shaped) 
        ELSE 0 
      END AS won_w,
      CASE 
        WHEN opp.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS won_full,
      CASE 
        WHEN opp.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) 
        ELSE 0 
      END AS won_custom,
      CASE 
        WHEN opp.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.l_weight) 
        ELSE 0 
      END AS won_linear,
      CASE 
        WHEN opp.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.first_net_arr) 
        ELSE 0 
      END AS won_first_net_arr,
      CASE 
        WHEN opp.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.u_net_arr) 
        ELSE 0 
      END AS won_u_net_arr,
      CASE 
        WHEN opp.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.w_net_arr) 
        ELSE 0 
      END AS won_w_net_arr,
      CASE 
        WHEN opp.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.full_net_arr) 
        ELSE 0 
      END AS won_full_net_arr,
      CASE 
        WHEN opp.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.custom_net_arr) 
        ELSE 0 
      END AS won_custom_net_arr,
      CASE 
        WHEN opp.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.linear_net_arr) 
        ELSE 0 
      END AS won_linear_net_arr
    FROM mart_crm_opportunity_stamped_hierarchy_hist opp
    LEFT JOIN mart_crm_attribution_touchpoint
      ON opp.dim_crm_opportunity_id=mart_crm_attribution_touchpoint.dim_crm_opportunity_id
    LEFT JOIN dim_crm_account
      ON opp.dim_crm_account_id=dim_crm_account.dim_crm_account_id
    {{dbt_utils.group_by(n=56)}}
    
), cohort_base_combined AS (
  
    SELECT
   --IDs
      dim_crm_person_id,
      person_base_with_tp.dim_crm_account_id,
      sfdc_record_id,
--       COALESCE (person_base_with_tp.dim_crm_touchpoint_id,opp_base_with_batp.dim_crm_touchpoint_id) AS dim_crm_touchpoint_id, 
      person_base_with_tp.dim_crm_touchpoint_id AS dim_crm_btp_touchpoint_id,
      opp_base_with_batp.dim_crm_touchpoint_id AS dim_crm_batp_touchpoint_id,
      dim_crm_opportunity_id,
  
  --Person Data
      email_hash,
      email_domain_type,
      true_inquiry_date,
      mql_date_first_pt,
      status,
      lead_source,
      is_mql,
      account_demographics_sales_segment,
      account_demographics_region,
      account_demographics_geo,
      account_demographics_area,
      account_demographics_upa_country,
      account_demographics_territory,
      is_first_order_available,
      person_order_type,
  
  --Opp Data
      opp_order_type,
      sales_qualified_source_name,
      deal_path_name,
      sales_type,
      sales_accepted_date,
      opp_created_date,
      close_date,
      is_won,
      is_sao,
      new_logo_count,
      net_arr,
      is_net_arr_closed_deal,
      crm_opp_owner_sales_segment_stamped,
      crm_opp_owner_region_stamped,
      crm_opp_owner_area_stamped,
      crm_opp_owner_geo_stamped,
      parent_crm_account_upa_country,
      parent_crm_account_territory,
  
  --Touchpoint Data
      person_base_with_tp.bizible_touchpoint_date AS btp_bizible_touchpoint_date,
      opp_base_with_batp.bizible_touchpoint_date AS batp_bizible_touchpoint_date,
      person_base_with_tp.bizible_touchpoint_position AS btp_bizibletouchpoint_position,
      opp_base_with_batp.bizible_touchpoint_position AS batp_bizible_touchpoint_position,
      person_base_with_tp.bizible_touchpoint_source AS btp_bizible_touchpoint_source,
      opp_base_with_batp.bizible_touchpoint_source AS batp_bizible_touchpoint_source,
      person_base_with_tp.bizible_touchpoint_type AS btp_bizible_touchpoint_type,
      opp_base_with_batp.bizible_touchpoint_type AS batp_bizible_touchpoint_type,
      person_base_with_tp.bizible_ad_campaign_name AS btp_bizible_ad_campaign_name,
      opp_base_with_batp.bizible_ad_campaign_name AS batp_bizible_ad_campaign_name,
      person_base_with_tp.bizible_form_url AS btp_bizible_form_url,
      opp_base_with_batp.bizible_form_url AS batp_bizible_form_url,
      person_base_with_tp.bizible_landing_page AS btp_bizible_landing_page,
      opp_base_with_batp.bizible_landing_page AS batp_bizible_landing_page,
      person_base_with_tp.bizible_form_url_raw AS btp_bizible_form_url_raw,
      opp_base_with_batp.bizible_form_url_raw AS batp_bizible_form_url_raw,
      person_base_with_tp.bizible_landing_page_raw AS btp_bizible_landing_page_raw,
      opp_base_with_batp.bizible_landing_page_raw AS batp_bizible_landing_page_raw,
      person_base_with_tp.bizible_marketing_channel AS btp_bizible_marketing_channel,
      opp_base_with_batp.bizible_marketing_channel AS batp_bizible_marketing_channel,
      person_base_with_tp.bizible_marketing_channel_path AS btp_bizible_marketing_channel_path,
      opp_base_with_batp.bizible_marketing_channel_path AS batp_bizible_marketing_channel_path,
      person_base_with_tp.bizible_medium AS btp_bizible_medium,
      opp_base_with_batp.bizible_medium AS batp_bizible_medium,
      person_base_with_tp.bizible_referrer_page AS btp_bizible_referrer_page,
      opp_base_with_batp.bizible_referrer_page AS batp_bizible_referrer_page,
      person_base_with_tp.bizible_referrer_page_raw AS btp_bizible_referrer_page_raw,
      opp_base_with_batp.bizible_referrer_page_raw AS batp_bizible_referrer_page_raw,
      person_base_with_tp.bizible_integrated_campaign_grouping AS btp_bizible_integrated_campaign_grouping,
      opp_base_with_batp.bizible_integrated_campaign_grouping AS batp_bizible_integrated_campaign_grouping,
      person_base_with_tp.touchpoint_segment AS btp_bizible_touchpoint_segment,
      opp_base_with_batp.touchpoint_segment AS batp_bizible_touchpoint_segment,
      person_base_with_tp.gtm_motion AS btp_bizible_gtm_motion,
      opp_base_with_batp.gtm_motion AS batp_bizible_gtm_motion,
      person_base_with_tp.pipe_name AS btp_bizible_pipe_name,
      opp_base_with_batp.pipe_name AS batp_bizible_pipe_name,
      person_base_with_tp.is_dg_influenced AS btp_is_dg_influenced,
      opp_base_with_batp.is_dg_influenced AS batp_is_dg_influenced,
      person_base_with_tp.is_dg_sourced AS btp_is_dg_sourced,
      opp_base_with_batp.is_dg_sourced AS batp_is_dg_sourced,
      person_base_with_tp.bizible_count_first_touch AS btp_bizible_count_first_touch,
      opp_base_with_batp.bizible_count_first_touch AS batp_bizible_count_first_touch,
      person_base_with_tp.bizible_count_lead_creation_touch AS btp_bizible_count_lead_creation_touch,
      opp_base_with_batp.bizible_count_lead_creation_touch AS batp_bizible_count_lead_creation_touch,
      person_base_with_tp.bizible_count_u_shaped AS btp_bizible_count_u_shaped,
      opp_base_with_batp.bizible_count_u_shaped AS batp_bizible_count_u_shaped,
      person_base_with_tp.is_fmm_influenced AS btp_is_fmm_influenced,
      opp_base_with_batp.is_fmm_influenced AS batp_is_fmm_influenced,
      person_base_with_tp.is_fmm_sourced AS btp_is_fmm_sourced,
      opp_base_with_batp.is_fmm_sourced AS batp_is_fmm_sourced,
      new_lead_created_sum,
      count_true_inquiry,
      inquiry_sum, 
      mql_sum,
      accepted_sum,
      new_mql_sum,
      new_accepted_sum,
      bizible_count_w_shaped,
      bizible_count_custom_model,
      bizible_weight_custom_model,
      bizible_weight_u_shaped,
      bizible_weight_w_shaped,
      bizible_weight_full_path,
      bizible_weight_first_touch,
      influenced_opportunity_id,
      first_opp_created,
      u_shaped_opp_created,
      w_shaped_opp_created,
      full_shaped_opp_created,
      custom_opp_created,
      linear_opp_created,
      first_net_arr,
      u_net_arr,
      w_net_arr,
      full_net_arr,
      custom_net_arr,
      linear_net_arr,
      first_sao,
      u_shaped_sao,
      w_shaped_sao,
      full_shaped_sao,
      custom_sao,
      linear_sao,
      pipeline_first_net_arr,
      pipeline_u_net_arr,
      pipeline_w_net_arr,
      pipeline_full_net_arr,
      pipeline_custom_net_arr,
      pipeline_linear_net_arr,
      won_first,
      won_u,
      won_w,
      won_full,
      won_custom,
      won_linear,
      won_first_net_arr,
      won_u_net_arr,
      won_w_net_arr,
      won_full_net_arr,
      won_custom_net_arr,
      won_linear_net_arr
  FROM person_base_with_tp
  FULL JOIN opp_base_with_batp
    ON person_base_with_tp.dim_crm_account_id=opp_base_with_batp.dim_crm_account_id

), intermediate AS (

  SELECT DISTINCT
    cohort_base_combined.*,
    --inquiry_date fields
    inquiry_date.fiscal_year                     AS inquiry_date_range_year,
    inquiry_date.fiscal_quarter_name_fy          AS inquiry_date_range_quarter,
    DATE_TRUNC(month, inquiry_date.date_actual)  AS inquiry_date_range_month,
    inquiry_date.first_day_of_week               AS inquiry_date_range_week,
    inquiry_date.date_id                         AS inquiry_date_range_id,
  
    --mql_date fields
    mql_date.fiscal_year                     AS mql_date_range_year,
    mql_date.fiscal_quarter_name_fy          AS mql_date_range_quarter,
    DATE_TRUNC(month, mql_date.date_actual)  AS mql_date_range_month,
    mql_date.first_day_of_week               AS mql_date_range_week,
    mql_date.date_id                         AS mql_date_range_id,
  
    --opp_create_date fields
    opp_create_date.fiscal_year                     AS opportunity_created_date_range_year,
    opp_create_date.fiscal_quarter_name_fy          AS opportunity_created_date_range_quarter,
    DATE_TRUNC(month, opp_create_date.date_actual)  AS opportunity_created_date_range_month,
    opp_create_date.first_day_of_week               AS opportunity_created_date_range_week,
    opp_create_date.date_id                         AS opportunity_created_date_range_id,
  
    --sao_date fields
    sao_date.fiscal_year                     AS sao_date_range_year,
    sao_date.fiscal_quarter_name_fy          AS sao_date_range_quarter,
    DATE_TRUNC(month, sao_date.date_actual)  AS sao_date_range_month,
    sao_date.first_day_of_week               AS sao_date_range_week,
    sao_date.date_id                         AS sao_date_range_id,
  
    --closed_date fields
    closed_date.fiscal_year                     AS closed_date_range_year,
    closed_date.fiscal_quarter_name_fy          AS closed_date_range_quarter,
    DATE_TRUNC(month, closed_date.date_actual)  AS closed_date_range_month,
    closed_date.first_day_of_week               AS closed_date_range_week,
    closed_date.date_id                         AS closed_date_range_id,

    --batp_date fields
    batp_date.fiscal_year                     AS batp_date_range_year,
    batp_date.fiscal_quarter_name_fy          AS batp_date_range_quarter,
    DATE_TRUNC(month, batp_date.date_actual)  AS batp_date_range_month,
    batp_date.first_day_of_week               AS batp_date_range_week,
    batp_date.date_id                         AS batp_date_range_id,
  
    --btp_date fields
    btp_date.fiscal_year                     AS btp_date_range_year,
    btp_date.fiscal_quarter_name_fy          AS btp_date_range_quarter,
    DATE_TRUNC(month, btp_date.date_actual)  AS btp_date_range_month,
    btp_date.first_day_of_week               AS btp_date_range_week,
    btp_date.date_id                         AS btp_date_range_id
  FROM cohort_base_combined
  LEFT JOIN dim_date AS inquiry_date
    ON cohort_base_combined.true_inquiry_date = inquiry_date.date_day
  LEFT JOIN dim_date AS mql_date
    ON cohort_base_combined.mql_date_first_pt = mql_date.date_day
  LEFT JOIN dim_date AS opp_create_date
    ON cohort_base_combined.opp_created_date = opp_create_date.date_day
  LEFT JOIN dim_date AS sao_date
    ON cohort_base_combined.sales_accepted_date = sao_date.date_day
  LEFT JOIN dim_date AS closed_date
    ON cohort_base_combined.close_date = closed_date.date_day
  LEFT JOIN dim_date AS btp_date
    ON cohort_base_combined.btp_bizible_touchpoint_date = btp_date.date_day
  LEFT JOIN dim_date AS batp_date
    ON cohort_base_combined.batp_bizible_touchpoint_date = batp_date.date_day

), final AS (

    SELECT DISTINCT *
    FROM intermediate

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@lisvinueza",
    created_date="2023-02-15",
    updated_date="2023-05-08",
  ) }}
