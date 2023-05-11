{{ config(
    tags=["mnpi_exception"],
    materialized="table"
) }}


{{ simple_cte([
    ('person_base','mart_crm_person'),
    ('dim_crm_person','dim_crm_person'),
    ('mart_crm_opportunity_stamped_hierarchy_hist', 'mart_crm_opportunity_stamped_hierarchy_hist'), 
    ('mart_crm_touchpoint', 'mart_crm_touchpoint'),
    ('mart_crm_attribution_touchpoint','mart_crm_attribution_touchpoint'),
    ('dim_crm_account', 'dim_crm_account'),
    ('sfdc_bizible_touchpoint','sfdc_bizible_touchpoint'),
    ('sfdc_bizible_attribution_touchpoint_xf','sfdc_bizible_attribution_touchpoint_xf'),
    ('dim_date','dim_date')
]) }}

,  mart_crm_touchpoint_base AS (

  SELECT 
    mart_crm_touchpoint.*,
    COALESCE(mart_crm_touchpoint.dim_crm_person_id,'null')||'-'||COALESCE(mart_crm_touchpoint.dim_campaign_id,'null')||'-'||COALESCE(mart_crm_touchpoint.sfdc_record_id,'null')||'-'||TO_CHAR(sfdc_bizible_touchpoint.bizible_touchpoint_date) AS tp_unique_id,
  FROM mart_crm_touchpoint
  LEFT JOIN sfdc_bizible_touchpoint
    ON mart_crm_touchpoint.dim_crm_touchpoint_id=sfdc_bizible_touchpoint.touchpoint_id

), mart_crm_attribution_touchpoint_base AS (
  
  SELECT 
    mart_crm_attribution_touchpoint.*,
    COALESCE(mart_crm_attribution_touchpoint.dim_crm_person_id,'null')||'-'||COALESCE(mart_crm_attribution_touchpoint.dim_campaign_id,'null')||'-'||COALESCE(mart_crm_attribution_touchpoint.sfdc_record_id,'null')||'-'||TO_CHAR(sfdc_bizible_attribution_touchpoint_xf.bizible_touchpoint_date) AS tp_unique_id,
  FROM mart_crm_attribution_touchpoint
  LEFT JOIN sfdc_bizible_attribution_touchpoint_xf
    ON mart_crm_attribution_touchpoint.dim_crm_touchpoint_id=sfdc_bizible_attribution_touchpoint_xf.touchpoint_id

) , upa_base AS ( --pulls every account and it's UPA
  
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

), tp_base AS (  
  
    SELECT DISTINCT
    --IDs
      COALESCE(mart_crm_attribution_touchpoint.dim_crm_person_id,mart_crm_touchpoint.dim_crm_person_id) AS dim_crm_person_id,
      COALESCE(mart_crm_attribution_touchpoint.dim_crm_touchpoint_id,mart_crm_touchpoint.dim_crm_touchpoint_id) AS dim_crm_touchpoint_id, 
  
    --TP Data
      COALESCE(mart_crm_attribution_touchpoint.bizible_touchpoint_date,mart_crm_touchpoint.bizible_touchpoint_date) AS bizible_touchpoint_date, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_touchpoint_position,mart_crm_touchpoint.bizible_touchpoint_position) AS bizible_touchpoint_position, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_touchpoint_source,mart_crm_touchpoint.bizible_touchpoint_source) AS bizible_touchpoint_source, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_touchpoint_type,mart_crm_touchpoint.bizible_touchpoint_type) AS bizible_touchpoint_type, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_ad_campaign_name,mart_crm_touchpoint.bizible_ad_campaign_name) AS bizible_ad_campaign_name, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_form_url,mart_crm_touchpoint.bizible_form_url) AS bizible_form_url, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_landing_page,mart_crm_touchpoint.bizible_landing_page) AS bizible_landing_page, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_form_url_raw,mart_crm_touchpoint.bizible_form_url_raw) AS bizible_form_url_raw, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_landing_page_raw,mart_crm_touchpoint.bizible_landing_page_raw) AS bizible_landing_page_raw, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_marketing_channel,mart_crm_touchpoint.bizible_marketing_channel) AS bizible_marketing_channel, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_marketing_channel_path,mart_crm_touchpoint.bizible_marketing_channel_path) AS bizible_marketing_channel_path, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_medium,mart_crm_touchpoint.bizible_medium) AS bizible_medium, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_referrer_page,mart_crm_touchpoint.bizible_referrer_page) AS bizible_referrer_page, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_referrer_page_raw,mart_crm_touchpoint.bizible_referrer_page_raw) AS bizible_referrer_page_raw, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_integrated_campaign_grouping,mart_crm_touchpoint.bizible_integrated_campaign_grouping) AS bizible_integrated_campaign_grouping, 
      COALESCE(mart_crm_attribution_touchpoint.touchpoint_segment,mart_crm_touchpoint.touchpoint_segment) AS touchpoint_segment, 
      COALESCE(mart_crm_attribution_touchpoint.gtm_motion,mart_crm_touchpoint.gtm_motion) AS gtm_motion, 
      COALESCE(mart_crm_attribution_touchpoint.pipe_name,mart_crm_touchpoint.pipe_name) AS pipe_name, 
      COALESCE(mart_crm_attribution_touchpoint.is_dg_influenced,mart_crm_touchpoint.is_dg_influenced) AS is_dg_influenced, 
      COALESCE(mart_crm_attribution_touchpoint.is_dg_sourced,mart_crm_touchpoint.is_dg_sourced) AS is_dg_sourced, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_count_first_touch,mart_crm_touchpoint.bizible_count_first_touch) AS bizible_count_first_touch, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_count_lead_creation_touch,mart_crm_touchpoint.bizible_count_lead_creation_touch) AS bizible_count_lead_creation_touch, 
      COALESCE(mart_crm_attribution_touchpoint.bizible_count_u_shaped,mart_crm_touchpoint.bizible_count_u_shaped) AS bizible_count_u_shaped, 
      COALESCE(mart_crm_attribution_touchpoint.is_fmm_influenced,mart_crm_touchpoint.is_fmm_influenced) AS is_fmm_influenced, 
      COALESCE(mart_crm_attribution_touchpoint.is_fmm_sourced,mart_crm_touchpoint.is_fmm_sourced) AS is_fmm_sourced,
      mart_crm_touchpoint.bizible_count_lead_creation_touch AS new_lead_created_sum,
      mart_crm_touchpoint.count_true_inquiry AS count_true_inquiry,
      mart_crm_touchpoint.count_inquiry AS inquiry_sum, 
      mart_crm_touchpoint.pre_mql_weight AS mql_sum,
      mart_crm_touchpoint.count_accepted AS accepted_sum,
      mart_crm_touchpoint.count_net_new_mql AS new_mql_sum,
      mart_crm_touchpoint.count_net_new_accepted AS new_accepted_sum,
      mart_crm_attribution_touchpoint.is_sao,
      mart_crm_attribution_touchpoint.is_won,
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
        WHEN is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS first_sao,
      CASE 
        WHEN is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_u_shaped) 
        ELSE 0 
      END AS u_shaped_sao,
      CASE 
        WHEN is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_w_shaped) 
        ELSE 0 
      END AS w_shaped_sao,
      CASE 
        WHEN is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS full_shaped_sao,
      CASE 
        WHEN is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) 
        ELSE 0 
      END AS custom_sao,
      CASE 
        WHEN is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.l_weight) 
        ELSE 0 
      END AS linear_sao,
      CASE 
        WHEN is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.u_net_arr) 
        ELSE 0 
      END AS pipeline_first_net_arr,
      CASE 
        WHEN is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.u_net_arr) 
        ELSE 0 
        END AS pipeline_u_net_arr,
      CASE 
        WHEN is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.w_net_arr) 
        ELSE 0 
      END AS pipeline_w_net_arr,
      CASE 
        WHEN is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.full_net_arr) 
        ELSE 0 
      END AS pipeline_full_net_arr,
      CASE 
        WHEN is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.custom_net_arr) 
        ELSE 0 
      END AS pipeline_custom_net_arr,
      CASE 
        WHEN is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.linear_net_arr) 
        ELSE 0 
      END AS pipeline_linear_net_arr,
      CASE 
        WHEN is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS won_first,
      CASE 
        WHEN is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_u_shaped) 
        ELSE 0 
      END AS won_u,
      CASE 
        WHEN is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_w_shaped) 
        ELSE 0 
      END AS won_w,
      CASE 
        WHEN is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS won_full,
      CASE 
        WHEN is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) 
        ELSE 0 
      END AS won_custom,
      CASE 
        WHEN is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.l_weight) 
        ELSE 0 
      END AS won_linear,
      CASE 
        WHEN is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.first_net_arr) 
        ELSE 0 
      END AS won_first_net_arr,
      CASE 
        WHEN is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.u_net_arr) 
        ELSE 0 
      END AS won_u_net_arr,
      CASE 
        WHEN is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.w_net_arr) 
        ELSE 0 
      END AS won_w_net_arr,
      CASE 
        WHEN is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.full_net_arr) 
        ELSE 0 
      END AS won_full_net_arr,
      CASE 
        WHEN is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.custom_net_arr) 
        ELSE 0 
      END AS won_custom_net_arr,
      CASE 
        WHEN is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.linear_net_arr) 
        ELSE 0 
      END AS won_linear_net_arr
    FROM mart_crm_touchpoint
    LEFT JOIN mart_crm_attribution_touchpoint
      ON mart_crm_touchpoint.tp_unique_id=mart_crm_attribution_touchpoint.tp_unique_id 
    {{dbt_utils.group_by(n=36)}}
  
), person_opp_base AS (

    SELECT DISTINCT
   --IDs
      person_base_2.dim_crm_person_id,
      person_base_2.dim_crm_account_id,
      person_base_2.sfdc_record_id,
      mart_crm_opportunity_stamped_hierarchy_hist.dim_crm_opportunity_id,
  
   --Person Data
      person_base_2.email_hash,
      person_base_2.email_domain_type,
      person_base_2.true_inquiry_date,
      person_base_2.mql_date_first_pt,
      person_base_2.status,
      person_base_2.lead_source,
      person_base_2.is_mql,
      person_base_2.account_demographics_sales_segment,
      person_base_2.account_demographics_region,
      person_base_2.account_demographics_geo,
      person_base_2.account_demographics_area,
      person_base_2.account_demographics_upa_country,
      person_base_2.account_demographics_territory,
      accounts_with_first_order_opps.is_first_order_available,
      person_order_type,
  
   --Opp Data
      mart_crm_opportunity_stamped_hierarchy_hist.order_type,
      mart_crm_opportunity_stamped_hierarchy_hist.sales_qualified_source_name,
      mart_crm_opportunity_stamped_hierarchy_hist.deal_path_name,
      mart_crm_opportunity_stamped_hierarchy_hist.sales_type,
      mart_crm_opportunity_stamped_hierarchy_hist.sales_accepted_date,
      mart_crm_opportunity_stamped_hierarchy_hist.created_date,
      mart_crm_opportunity_stamped_hierarchy_hist.close_date,
      mart_crm_opportunity_stamped_hierarchy_hist.is_won,
      mart_crm_opportunity_stamped_hierarchy_hist.is_sao,
      mart_crm_opportunity_stamped_hierarchy_hist.new_logo_count,
      mart_crm_opportunity_stamped_hierarchy_hist.net_arr,
      mart_crm_opportunity_stamped_hierarchy_hist.is_net_arr_closed_deal,
      mart_crm_opportunity_stamped_hierarchy_hist.crm_opp_owner_sales_segment_stamped,
      mart_crm_opportunity_stamped_hierarchy_hist.crm_opp_owner_region_stamped,
      mart_crm_opportunity_stamped_hierarchy_hist.crm_opp_owner_area_stamped,
      mart_crm_opportunity_stamped_hierarchy_hist.crm_opp_owner_geo_stamped,
      mart_crm_opportunity_stamped_hierarchy_hist.parent_crm_account_demographics_upa_country,
      mart_crm_opportunity_stamped_hierarchy_hist.parent_crm_account_demographics_territory
    FROM person_base person_base_2
    FULL OUTER JOIN mart_crm_opportunity_stamped_hierarchy_hist
      ON person_base_2.dim_crm_account_id=mart_crm_opportunity_stamped_hierarchy_hist.dim_crm_account_id
    INNER JOIN dim_crm_person
      ON person_base_2.dim_crm_person_id = dim_crm_person.dim_crm_person_id
    LEFT JOIN upa_base
      ON person_base_2.dim_crm_account_id = upa_base.dim_crm_account_id
    LEFT JOIN dim_crm_account
      ON person_base_2.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    LEFT JOIN person_order_type_final
      ON person_base_2.email_hash = person_order_type_final.email_hash
  
), person_opp_tp_combined AS (
  
    SELECT DISTINCT
    --IDs
      person_opp_base.dim_crm_person_id,
      dim_crm_account_id,
      sfdc_record_id,
      dim_crm_touchpoint_id, 
      dim_crm_opportunity_id,
      CASE
          WHEN tp_base.dim_crm_touchpoint_id IS NOT null THEN person_opp_base.dim_crm_opportunity_id
          ELSE null
      END AS influenced_opportunity_id,
  
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
      order_type,
      sales_qualified_source_name,
      deal_path_name,
      sales_type,
      sales_accepted_date,
      created_date,
      close_date,
      person_opp_base.is_won,
      person_opp_base.is_sao,
      new_logo_count,
      net_arr,
      is_net_arr_closed_deal,
      crm_opp_owner_sales_segment_stamped,
      crm_opp_owner_region_stamped,
      crm_opp_owner_area_stamped,
      crm_opp_owner_geo_stamped,
      parent_crm_account_demographics_upa_country,
      parent_crm_account_demographics_territory,
       
  
    --TP Data
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
    FROM person_opp_base
    LEFT JOIN tp_base
      ON person_opp_base.dim_crm_person_id=tp_base.dim_crm_person_id

), intermediate AS (

  SELECT DISTINCT
    person_opp_tp_combined.*,
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
  
    --tp_date fields
    tp_date.fiscal_year                     AS btp_date_range_year,
    tp_date.fiscal_quarter_name_fy          AS btp_date_range_quarter,
    DATE_TRUNC(month, tp_date.date_actual)  AS btp_date_range_month,
    tp_date.first_day_of_week               AS btp_date_range_week,
    tp_date.date_id                         AS btp_date_range_id
  FROM person_opp_tp_combined
  LEFT JOIN dim_date AS inquiry_date
    ON person_opp_tp_combined.true_inquiry_date = inquiry_date.date_day
  LEFT JOIN dim_date AS mql_date
    ON person_opp_tp_combined.mql_date_first_pt = mql_date.date_day
  LEFT JOIN dim_date AS opp_create_date
    ON person_opp_tp_combined.created_date = opp_create_date.date_day
  LEFT JOIN dim_date AS sao_date
    ON person_opp_tp_combined.sales_accepted_date = sao_date.date_day
  LEFT JOIN dim_date AS closed_date
    ON person_opp_tp_combined.close_date = closed_date.date_day
  LEFT JOIN dim_date AS tp_date
    ON person_opp_tp_combined.bizible_touchpoint_date = tp_date.date_day

), final AS (

    SELECT DISTINCT *
    FROM intermediate

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-05-11",
    updated_date="2023-05-11",
  ) }}
