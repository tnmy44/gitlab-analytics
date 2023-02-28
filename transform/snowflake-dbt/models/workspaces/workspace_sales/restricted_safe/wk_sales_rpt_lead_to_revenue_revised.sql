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
      ON person_base.dim_crm_account_id=upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN opportunity_base
      ON upa_base.dim_parent_crm_account_id=opportunity_base.dim_parent_crm_account_id

), person_order_type_final AS (

    SELECT DISTINCT
      email_hash,
      dim_crm_opportunity_id,
      dim_parent_crm_account_id,
      dim_crm_account_id,
      person_order_type
    FROM person_order_type_base
    WHERE person_order_type_number=1

), cohort_base AS (

    SELECT DISTINCT
      opp.order_type AS opp_order_type,
      person_base.email_hash,
      person_base.email_domain_type,
      person_base.true_inquiry_date,
      person_base.mql_date_first_pt,
      person_base.status,
      person_base.lead_source,
      person_base.dim_crm_person_id,
      person_base.dim_crm_account_id,
      person_base.is_mql,
      dim_crm_person.sfdc_record_id,
      person_base.account_demographics_sales_segment,
      person_base.account_demographics_region,
      person_base.account_demographics_geo,
      person_base.account_demographics_area,
      person_base.account_demographics_upa_country,
      person_base.account_demographics_territory,
      is_first_order_available,
      person_order_type_final.person_order_type,
      
      opp.sales_qualified_source_name,
      opp.deal_path_name,
      opp.sales_type,
      opp.dim_crm_opportunity_id,
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
      opp.parent_crm_account_demographics_territory
    FROM person_base
    INNER JOIN dim_crm_person
      ON person_base.dim_crm_person_id=dim_crm_person.dim_crm_person_id
    LEFT JOIN upa_base
    ON person_base.dim_crm_account_id=upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN mart_crm_opportunity_stamped_hierarchy_hist opp
      ON upa_base.dim_parent_crm_account_id=opp.dim_parent_crm_account_id
    LEFT JOIN person_order_type_final
      ON person_base.email_hash=person_order_type_final.email_hash
  
  ), cohort_base_with_btp AS (

 SELECT DISTINCT
    --Key IDs
    cohort_base.email_hash,
    cohort_base.dim_crm_person_id,
    cohort_base.dim_crm_opportunity_id,
    mart_crm_touchpoint.dim_crm_touchpoint_id,
    cohort_base.sfdc_record_id,
  
    --person data
    CASE 
      WHEN cohort_base.person_order_type IS null AND cohort_base.opp_order_type IS null THEN 'Missing order_type_name'
      WHEN cohort_base.person_order_type IS null THEN cohort_base.opp_order_type
      ELSE person_order_type
    END AS person_order_type,
    cohort_base.lead_source,    
    cohort_base.email_domain_type,
    cohort_base.is_mql,
    cohort_base.account_demographics_sales_segment,
    cohort_base.account_demographics_geo,
    cohort_base.account_demographics_region,
    cohort_base.account_demographics_area,
    cohort_base.account_demographics_upa_country,
    cohort_base.account_demographics_territory,
    cohort_base.true_inquiry_date,
    cohort_base.mql_date_first_pt,
  
    --opportunity data
    cohort_base.opp_created_date,
    cohort_base.sales_accepted_date,
    cohort_base.close_date,
    cohort_base.is_sao,
    cohort_base.is_won,
    cohort_base.new_logo_count,
    cohort_base.net_arr,
    cohort_base.is_net_arr_closed_deal,
    cohort_base.opp_order_type,
    cohort_base.sales_qualified_source_name,
    cohort_base.deal_path_name,
    cohort_base.sales_type,
    cohort_base.crm_opp_owner_geo_stamped,
    cohort_base.crm_opp_owner_sales_segment_stamped,
    cohort_base.crm_opp_owner_region_stamped,
    cohort_base.crm_opp_owner_area_stamped,
    cohort_base.parent_crm_account_demographics_upa_country,
    cohort_base.parent_crm_account_demographics_territory,
  
    --touchpoint data
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
  FROM cohort_base
  LEFT JOIN mart_crm_touchpoint
    ON mart_crm_touchpoint.email_hash=cohort_base.email_hash
  {{ dbt_utils.group_by(n=61) }}
    
), cohort_base_with_batp AS (
  
  SELECT DISTINCT
    --Key IDs
    cohort_base.email_hash,
    cohort_base.dim_crm_person_id,
    cohort_base.dim_crm_opportunity_id,
    mart_crm_attribution_touchpoint.dim_crm_touchpoint_id,
    cohort_base.sfdc_record_id,
  
    --person data
    CASE 
      WHEN cohort_base.person_order_type IS null AND cohort_base.opp_order_type IS null THEN 'Missing order_type_name'
      WHEN cohort_base.person_order_type IS null THEN cohort_base.opp_order_type
      ELSE person_order_type
    END AS person_order_type,
    cohort_base.lead_source,    
    cohort_base.email_domain_type,
    cohort_base.is_mql,
    cohort_base.account_demographics_sales_segment,
    cohort_base.account_demographics_geo,
    cohort_base.account_demographics_region,
    cohort_base.account_demographics_area,
    cohort_base.account_demographics_upa_country,
    cohort_base.account_demographics_territory,
    cohort_base.true_inquiry_date,
    cohort_base.mql_date_first_pt,
  
    --opportunity data
    cohort_base.opp_created_date,
    cohort_base.sales_accepted_date,
    cohort_base.close_date,
    cohort_base.is_sao,
    cohort_base.is_won,
    cohort_base.new_logo_count,
    cohort_base.net_arr,
    cohort_base.is_net_arr_closed_deal,
    cohort_base.opp_order_type,
    cohort_base.sales_qualified_source_name,
    cohort_base.deal_path_name,
    cohort_base.sales_type,
    cohort_base.crm_opp_owner_geo_stamped,
    cohort_base.crm_opp_owner_sales_segment_stamped,
    cohort_base.crm_opp_owner_region_stamped,
    cohort_base.crm_opp_owner_area_stamped,
    cohort_base.parent_crm_account_demographics_upa_country,
    cohort_base.parent_crm_account_demographics_territory,
  
    --touchpoint data
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
        WHEN mart_crm_attribution_touchpoint.dim_crm_touchpoint_id IS NOT null THEN cohort_base.dim_crm_opportunity_id
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
      WHEN cohort_base.is_sao = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
      ELSE 0 
    END AS first_sao,
    CASE 
      WHEN cohort_base.is_sao = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.bizible_count_u_shaped) 
      ELSE 0 
    END AS u_shaped_sao,
    CASE 
      WHEN cohort_base.is_sao = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.bizible_count_w_shaped) 
      ELSE 0 
    END AS w_shaped_sao,
    CASE 
      WHEN cohort_base.is_sao = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
      ELSE 0 
    END AS full_shaped_sao,
    CASE 
      WHEN cohort_base.is_sao = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) 
      ELSE 0 
    END AS custom_sao,
    CASE 
      WHEN cohort_base.is_sao = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.l_weight) 
      ELSE 0 
    END AS linear_sao,
    CASE 
      WHEN cohort_base.is_sao = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.u_net_arr) 
      ELSE 0 
    END AS pipeline_first_net_arr,
    CASE 
      WHEN cohort_base.is_sao = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.u_net_arr) 
      ELSE 0 
      END AS pipeline_u_net_arr,
    CASE 
      WHEN cohort_base.is_sao = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.w_net_arr) 
      ELSE 0 
    END AS pipeline_w_net_arr,
    CASE 
      WHEN cohort_base.is_sao = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.full_net_arr) 
      ELSE 0 
    END AS pipeline_full_net_arr,
    CASE 
      WHEN cohort_base.is_sao = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.custom_net_arr) 
      ELSE 0 
    END AS pipeline_custom_net_arr,
    CASE 
      WHEN cohort_base.is_sao = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.linear_net_arr) 
      ELSE 0 
    END AS pipeline_linear_net_arr,
    CASE 
      WHEN cohort_base.is_won = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
      ELSE 0 
    END AS won_first,
    CASE 
      WHEN cohort_base.is_won = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.bizible_count_u_shaped) 
      ELSE 0 
    END AS won_u,
    CASE 
      WHEN cohort_base.is_won = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.bizible_count_w_shaped) 
      ELSE 0 
    END AS won_w,
    CASE 
      WHEN cohort_base.is_won = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
      ELSE 0 
    END AS won_full,
    CASE 
      WHEN cohort_base.is_won = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) 
      ELSE 0 
    END AS won_custom,
    CASE 
      WHEN cohort_base.is_won = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.l_weight) 
      ELSE 0 
    END AS won_linear,
    CASE 
      WHEN cohort_base.is_won = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.first_net_arr) 
      ELSE 0 
    END AS won_first_net_arr,
    CASE 
      WHEN cohort_base.is_won = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.u_net_arr) 
      ELSE 0 
    END AS won_u_net_arr,
    CASE 
      WHEN cohort_base.is_won = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.w_net_arr) 
      ELSE 0 
    END AS won_w_net_arr,
    CASE 
      WHEN cohort_base.is_won = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.full_net_arr) 
      ELSE 0 
    END AS won_full_net_arr,
    CASE 
      WHEN cohort_base.is_won = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.custom_net_arr) 
      ELSE 0 
    END AS won_custom_net_arr,
    CASE 
      WHEN cohort_base.is_won = TRUE
        THEN SUM(mart_crm_attribution_touchpoint.linear_net_arr) 
      ELSE 0 
    END AS won_linear_net_arr
  FROM cohort_base
  LEFT JOIN mart_crm_attribution_touchpoint
    ON cohort_base.dim_crm_opportunity_id=mart_crm_attribution_touchpoint.dim_crm_opportunity_id
  {{ dbt_utils.group_by(n=69) }}
    
), cohort_base_with_touchpoints AS (
  
  SELECT DISTINCT
    --Key IDs
    email_hash,
    dim_crm_person_id,
    dim_crm_opportunity_id,
    dim_crm_touchpoint_id,
    sfdc_record_id,
  
    --person data
    person_order_type,
    lead_source,    
    email_domain_type,
    is_mql,
    account_demographics_sales_segment,
    account_demographics_geo,
    account_demographics_region,
    account_demographics_area,
    account_demographics_upa_country,
    account_demographics_territory,
    true_inquiry_date,
    mql_date_first_pt,
  
    --opportunity data
    opp_created_date,
    sales_accepted_date,
    close_date,
    is_sao,
    is_won,
    new_logo_count,
    net_arr,
    is_net_arr_closed_deal,
    opp_order_type,
    sales_qualified_source_name,
    deal_path_name,
    sales_type,
    crm_opp_owner_geo_stamped,
    crm_opp_owner_sales_segment_stamped,
    crm_opp_owner_region_stamped,
    crm_opp_owner_area_stamped,
    parent_crm_account_demographics_upa_country,
    parent_crm_account_demographics_territory,
  
    --touchpoint data
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
    null AS bizible_count_w_shaped,
    null AS bizible_count_custom_model,
    null AS bizible_weight_custom_model,
    null AS bizible_weight_u_shaped,
    null AS bizible_weight_w_shaped,
    null AS bizible_weight_full_path,
    null AS bizible_weight_first_touch,
    is_fmm_influenced,
    is_fmm_sourced,
    null AS influenced_opportunity_id,
    new_lead_created_sum,
    count_true_inquiry,
    inquiry_sum, 
    mql_sum,
    accepted_sum,
    new_mql_sum,
    new_accepted_sum,
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
  FROM cohort_base_with_btp
  UNION ALL
  SELECT DISTINCT
    --Key IDs
    email_hash,
    dim_crm_person_id,
    dim_crm_opportunity_id,
    dim_crm_touchpoint_id,
    sfdc_record_id,
  
    --person data
    person_order_type,
    lead_source,    
    email_domain_type,
    is_mql,
    account_demographics_sales_segment,
    account_demographics_geo,
    account_demographics_region,
    account_demographics_area,
    account_demographics_upa_country,
    account_demographics_territory,
    true_inquiry_date,
    mql_date_first_pt,
  
    --opportunity data
    opp_created_date,
    sales_accepted_date,
    close_date,
    is_sao,
    is_won,
    new_logo_count,
    net_arr,
    is_net_arr_closed_deal,
    opp_order_type,
    sales_qualified_source_name,
    deal_path_name,
    sales_type,
    crm_opp_owner_geo_stamped,
    crm_opp_owner_sales_segment_stamped,
    crm_opp_owner_region_stamped,
    crm_opp_owner_area_stamped,
    parent_crm_account_demographics_upa_country,
    parent_crm_account_demographics_territory,
  
    --touchpoint data
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
    bizible_count_w_shaped,
    bizible_count_custom_model,
    bizible_weight_custom_model,
    bizible_weight_u_shaped,
    bizible_weight_w_shaped,
    bizible_weight_full_path,
    bizible_weight_first_touch,
    is_fmm_influenced,
    is_fmm_sourced,
    influenced_opportunity_id,
    0 AS new_lead_created_sum,
    0 AS count_true_inquiry,
    0 AS inquiry_sum, 
    0 AS mql_sum,
    0 AS accepted_sum,
    0 AS new_mql_sum,
    0 AS new_accepted_sum,
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
  FROM cohort_base_with_batp

), final AS (

    SELECT DISTINCT *
    FROM cohort_base_with_touchpoints

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-02-15",
    updated_date="2023-02-15",
  ) }}