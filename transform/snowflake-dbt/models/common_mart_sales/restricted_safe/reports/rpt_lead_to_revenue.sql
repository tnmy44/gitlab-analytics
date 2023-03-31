{{ simple_cte([
    ('opportunity_base','mart_crm_opportunity'),
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
    FROM opportunity_base
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
      opportunity_base.dim_crm_opportunity_id,
      CASE 
         WHEN is_first_order_available = False AND opportunity_base.order_type = '1. New - First Order' THEN '3. Growth'
         WHEN is_first_order_available = False AND opportunity_base.order_type != '1. New - First Order' THEN opportunity_base.order_type
      ELSE '1. New - First Order'
      END AS person_order_type,
      ROW_NUMBER() OVER( PARTITION BY email_hash ORDER BY person_order_type) AS person_order_type_number
    FROM person_base
    FULL JOIN upa_base
      ON person_base.dim_crm_account_id = upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN opportunity_base
      ON upa_base.dim_parent_crm_account_id = opportunity_base.dim_parent_crm_account_id

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
      is_first_order_available,
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
    SUM(mart_crm_touchpoint.bizible_count_lead_creation_touch) AS new_lead_created_sum,
    SUM(mart_crm_touchpoint.count_true_inquiry) AS count_true_inquiry,
    SUM(mart_crm_touchpoint.count_inquiry) AS inquiry_sum, 
    SUM(mart_crm_touchpoint.pre_mql_weight) AS mql_sum,
    SUM(mart_crm_touchpoint.count_accepted) AS accepted_sum,
    SUM(mart_crm_touchpoint.count_net_new_mql) AS new_mql_sum,
    SUM(mart_crm_touchpoint.count_net_new_accepted) AS new_accepted_sum    
    FROM person_base
    INNER JOIN dim_crm_person
      ON person_base.dim_crm_person_id = dim_crm_person.dim_crm_person_id
    LEFT JOIN upa_base
    ON person_base.dim_crm_account_id = upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    LEFT JOIN person_order_type_final
      ON person_base.email_hash = person_order_type_final.email_hash
    LEFT JOIN mart_crm_touchpoint
      ON mart_crm_touchpoint.email_hash = person_base.email_hash
    {{dbt_utils.group_by(n=45)}}

  ), opp_base_wtih_batp AS (
    
    SELECT
    --IDs
      opp.dim_crm_opportunity_id,
      opp.dim_crm_account_id,
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
      opp.parent_crm_account_demographics_upa_country,
      opp.parent_crm_account_demographics_territory,
    
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
 	{{dbt_utils.group_by(n=55)}}
    
), cohort_base_combined AS (
  
    SELECT
   --IDs
      dim_crm_person_id,
      dim_crm_account_id,
      sfdc_record_id,
      dim_crm_touchpoint_id,
      null AS dim_crm_opportunity_id,
  
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
      null AS opp_order_type,
      null AS sales_qualified_source_name,
      null AS deal_path_name,
      null AS sales_type,
      null AS sales_accepted_date,
      null AS opp_created_date,
      null AS close_date,
      null AS is_won,
      null AS is_sao,
      null AS new_logo_count,
      null AS net_arr,
      null AS is_net_arr_closed_deal,
      null AS crm_opp_owner_sales_segment_stamped,
      null AS crm_opp_owner_region_stamped,
      null AS crm_opp_owner_area_stamped,
      null AS crm_opp_owner_geo_stamped,
      null AS parent_crm_account_demographics_upa_country,
      null AS parent_crm_account_demographics_territory,
  
  --Touchpoint Data
    touchpoint_type,
    bizible_touchpoint_date,
    bizible_touchpoint_position,
    bizible_touchpoint_source,
    bizible_touchpoint_type,
    bizible_ad_campaign_name,
    bizible_form_url,
    bizible_landing_page,
    bizible_form_url_raw,
    bizible_landing_page_raw,
    bizible_marketing_channel,
    bizible_marketing_channel_path,
    bizible_medium,
    bizible_referrer_page,
    bizible_referrer_page_raw,
    bizible_integrated_campaign_grouping,
    touchpoint_segment,
    gtm_motion,
    pipe_name,
    is_dg_influenced,
    is_dg_sourced,
    bizible_count_first_touch,
    bizible_count_lead_creation_touch,
    bizible_count_u_shaped,
    is_fmm_influenced,
    is_fmm_sourced,
    new_lead_created_sum,
    count_true_inquiry,
    inquiry_sum, 
    mql_sum,
    accepted_sum,
    new_mql_sum,
    new_accepted_sum,
    null AS bizible_count_w_shaped,
    null AS bizible_count_custom_model,
    null AS bizible_weight_custom_model,
    null AS bizible_weight_u_shaped,
    null AS bizible_weight_w_shaped,
    null AS bizible_weight_full_path,
    null AS bizible_weight_first_touch,
    null AS influenced_opportunity_id,
    0 AS first_opp_created,
    0 AS u_shaped_opp_created,
    0 AS w_shaped_opp_created,
    0 AS full_shaped_opp_created,
    0 AS custom_opp_created,
    0 AS linear_opp_created,
    0 AS first_net_arr,
    0 AS u_net_arr,
    0 AS w_net_arr,
    0 AS full_net_arr,
    0 AS custom_net_arr,
    0 AS linear_net_arr,
    0 AS first_sao,
    0 AS u_shaped_sao,
    0 AS w_shaped_sao,
    0 AS full_shaped_sao,
    0 AS custom_sao,
    0 AS linear_sao,
    0 AS pipeline_first_net_arr,
    0 AS pipeline_u_net_arr,
    0 AS pipeline_w_net_arr,
    0 AS pipeline_full_net_arr,
    0 AS pipeline_custom_net_arr,
    0 AS pipeline_linear_net_arr,
    0 AS won_first,
    0 AS won_u,
    0 AS won_w,
    0 AS won_full,
    0 AS won_custom,
    0 AS won_linear,
    0 AS won_first_net_arr,
    0 AS won_u_net_arr,
    0 AS won_w_net_arr,
    0 AS won_full_net_arr,
    0 AS won_custom_net_arr,
    0 AS won_linear_net_arr
  FROM person_base_with_tp
  UNION ALL
  SELECT
   --IDs
      null AS dim_crm_person_id,
      dim_crm_account_id,
      null AS sfdc_record_id,
      dim_crm_touchpoint_id,
      dim_crm_opportunity_id,
  
  --Person Data
      null AS email_hash,
      null AS email_domain_type,
      null AS true_inquiry_date,
      null AS mql_date_first_pt,
      null AS status,
      null AS lead_source,
      null AS is_mql,
      null AS account_demographics_sales_segment,
      null AS account_demographics_region,
      null AS account_demographics_geo,
      null AS account_demographics_area,
      null AS account_demographics_upa_country,
      null AS account_demographics_territory,
      null AS is_first_order_available,
      null AS person_order_type,
  
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
      parent_crm_account_demographics_upa_country,
      parent_crm_account_demographics_territory,
  
    --Touchpoint Data
      touchpoint_type,
      bizible_touchpoint_date,
      bizible_touchpoint_position,
      bizible_touchpoint_source,
      bizible_touchpoint_type,
      bizible_ad_campaign_name,
      bizible_form_url,
      bizible_landing_page,
      bizible_form_url_raw,
      bizible_landing_page_raw,
      bizible_marketing_channel,
      bizible_marketing_channel_path,
      bizible_medium,
      bizible_referrer_page,
      bizible_referrer_page_raw,
      bizible_integrated_campaign_grouping,
      touchpoint_segment,
      gtm_motion,
      pipe_name,
      is_dg_influenced,
      is_dg_sourced,
      bizible_count_first_touch,
      bizible_count_lead_creation_touch,
      bizible_count_u_shaped,
      is_fmm_influenced,
      is_fmm_sourced,
      0 AS new_lead_created_sum,
      0 AS count_true_inquiry,
      0 AS inquiry_sum, 
      0 AS mql_sum,
      0 AS accepted_sum,
      0 AS new_mql_sum,
      0 AS new_accepted_sum,
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
  FROM opp_base_wtih_batp

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

    --bizible_date fields
    bizible_date.fiscal_year                     AS bizible_date_range_year,
    bizible_date.fiscal_quarter_name_fy          AS bizible_date_range_quarter,
    DATE_TRUNC(month, bizible_date.date_actual)  AS bizible_date_range_month,
    bizible_date.first_day_of_week               AS bizible_date_range_week,
    bizible_date.date_id                         AS bizible_date_range_id
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
  LEFT JOIN dim_date AS bizible_date
    ON cohort_base_combined.bizible_touchpoint_date = bizible_date.date_day
    
), final AS (

    SELECT DISTINCT 
	intermediate.*
    FROM intermediate

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2022-10-05",
    updated_date="2023-03-31",
  ) }}