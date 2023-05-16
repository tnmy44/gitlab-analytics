{{ config(materialized='table') }}


{{ simple_cte([
    ('mart_crm_attribution_touchpoint','mart_crm_attribution_touchpoint'),
    ('wk_sales_sfdc_opportunity_snapshot_history_xf','wk_sales_sfdc_opportunity_snapshot_history_xf'),
    ('mart_crm_account','mart_crm_account'),
    ('attribution_touchpoint_offer_type','attribution_touchpoint_offer_type'),
    ('sfdc_bizible_attribution_touchpoint_snapshots_source', 'sfdc_bizible_attribution_touchpoint_snapshots_source'),
    ('dim_crm_user','dim_crm_user')
]) }}

,  wk_sales_sfdc_opportunity_snapshot_history_xf_base AS (
  
  SELECT
    wk_sales_sfdc_opportunity_snapshot_history_xf.opportunity_id AS dim_crm_opportunity_id,
    wk_sales_sfdc_opportunity_snapshot_history_xf.opportunity_category,
    wk_sales_sfdc_opportunity_snapshot_history_xf.sales_type,
    wk_sales_sfdc_opportunity_snapshot_history_xf.snapshot_order_type_stamped AS order_type,
    wk_sales_sfdc_opportunity_snapshot_history_xf.snapshot_sales_qualified_source AS sales_qualified_source_name,
    wk_sales_sfdc_opportunity_snapshot_history_xf.account_demographics_segment,
    wk_sales_sfdc_opportunity_snapshot_history_xf.account_demographics_geo,
    wk_sales_sfdc_opportunity_snapshot_history_xf.account_demographics_region,
    wk_sales_sfdc_opportunity_snapshot_history_xf.account_demographics_area,
    wk_sales_sfdc_opportunity_snapshot_history_xf.created_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf.sales_accepted_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf.pipeline_created_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf.close_date,
    CASE
        WHEN wk_sales_sfdc_opportunity_snapshot_history_xf.sales_accepted_date IS NOT NULL
          AND wk_sales_sfdc_opportunity_snapshot_history_xf.is_edu_oss = 0
          AND wk_sales_sfdc_opportunity_snapshot_history_xf.stage_name != '10-Duplicate'
            THEN TRUE
        ELSE FALSE
      END AS is_sao,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_won,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_web_portal_purchase,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_edu_oss,
    mart_crm_account.abm_tier,
    wk_sales_sfdc_opportunity_snapshot_history_xf.net_arr,
    COALESCE(
        wk_sales_sfdc_opportunity_snapshot_history_xf.is_eligible_created_pipeline_flag,
        CASE
         WHEN wk_sales_sfdc_opportunity_snapshot_history_xf.snapshot_order_type_stamped IN ('1. New - First Order' ,'2. New - Connected', '3. Growth')
           AND wk_sales_sfdc_opportunity_snapshot_history_xf.is_edu_oss = 0
           AND wk_sales_sfdc_opportunity_snapshot_history_xf.net_arr_created_date IS NOT NULL --
           AND wk_sales_sfdc_opportunity_snapshot_history_xf.snapshot_opportunity_category IN ('Standard','Internal Correction','Ramp Deal','Credit','Contract Reset')
           AND wk_sales_sfdc_opportunity_snapshot_history_xf.stage_name NOT IN ('00-Pre Opportunity','10-Duplicate', '9-Unqualified','0-Pending Acceptance')
           AND (wk_sales_sfdc_opportunity_snapshot_history_xf.net_arr > 0
             OR wk_sales_sfdc_opportunity_snapshot_history_xf.snapshot_opportunity_category = 'Credit')
           AND wk_sales_sfdc_opportunity_snapshot_history_xf.snapshot_sales_qualified_source  != 'Web Direct Generated'
           AND mart_crm_account.is_jihu_account = 0
          THEN 1
          ELSE 0
        END
        ) AS is_net_arr_pipeline_created,
    dim_crm_user.crm_user_sales_segment AS crm_opp_owner_sales_segment_stamped,
    dim_crm_user.crm_user_geo AS crm_opp_owner_geo_stamped,
    dim_crm_user.crm_user_region AS crm_opp_owner_region_stamped,
    dim_crm_user.crm_user_area AS crm_opp_owner_area_stamped,
    dim_crm_user.crm_user_business_unit AS crm_opp_owner_business_unit_stamped,
    wk_sales_sfdc_opportunity_snapshot_history_xf.snapshot_date AS opportunity_snapshot_date
  FROM wk_sales_sfdc_opportunity_snapshot_history_xf
  LEFT JOIN mart_crm_account
    ON wk_sales_sfdc_opportunity_snapshot_history_xf.account_id=mart_crm_account.dim_crm_account_id
  LEFT JOIN dim_crm_user
    ON wk_sales_sfdc_opportunity_snapshot_history_xf.owner_id=dim_crm_user.dim_crm_user_id

), bizible_weight_snapshot AS (
  
  SELECT DISTINCT
    touchpoint_id AS dim_crm_touchpoint_id,
    opportunity_id AS dim_crm_opportunity_id,
    dbt_valid_from AS touchpoint_weight_snapshot_date,
    bizible_touchpoint_date,
    bizible_count_first_touch,
    bizible_count_lead_creation_touch,
    bizible_attribution_percent_full_path,
    bizible_count_u_shaped,
    bizible_count_w_shaped,
    bizible_count_custom_model,
    utm_offertype
  FROM sfdc_bizible_attribution_touchpoint_snapshots_source
  
), combined_models AS (

  SELECT
  --IDs
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.dim_crm_opportunity_id,
    mart_crm_attribution_touchpoint.dim_crm_touchpoint_id,

  --Dates
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.created_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.sales_accepted_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.pipeline_created_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.close_date,
    mart_crm_attribution_touchpoint.bizible_touchpoint_date,
    bizible_weight_snapshot.touchpoint_weight_snapshot_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.opportunity_snapshot_date,
  
  --Dimensions
    mart_crm_attribution_touchpoint.bizible_marketing_channel,
    mart_crm_attribution_touchpoint.bizible_marketing_channel_path,
    attribution_touchpoint_offer_type.touchpoint_offer_type,
    attribution_touchpoint_offer_type.touchpoint_offer_type_grouped,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.order_type,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.opportunity_category,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.abm_tier,
    mart_crm_attribution_touchpoint.budget_holder,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.sales_qualified_source_name,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.account_demographics_segment,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.account_demographics_geo,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.account_demographics_region,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.account_demographics_area,

  --Sales Territory Fields
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.crm_opp_owner_sales_segment_stamped,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.crm_opp_owner_geo_stamped,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.crm_opp_owner_region_stamped,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.crm_opp_owner_area_stamped,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.crm_opp_owner_business_unit_stamped,

  --Facts
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.net_arr,

  --Weights
    bizible_weight_snapshot.bizible_count_first_touch,
    bizible_weight_snapshot.bizible_count_lead_creation_touch,
    bizible_weight_snapshot.bizible_attribution_percent_full_path,
    bizible_weight_snapshot.bizible_count_u_shaped,
    bizible_weight_snapshot.bizible_count_w_shaped,
    bizible_weight_snapshot.bizible_count_custom_model,

  --Flags
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_sao,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_won,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_net_arr_pipeline_created,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_web_portal_purchase,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_edu_oss
  FROM wk_sales_sfdc_opportunity_snapshot_history_xf_base
  LEFT JOIN mart_crm_attribution_touchpoint
    ON mart_crm_attribution_touchpoint.dim_crm_opportunity_id=wk_sales_sfdc_opportunity_snapshot_history_xf_base.dim_crm_opportunity_id
  LEFT JOIN bizible_weight_snapshot
    ON mart_crm_attribution_touchpoint.dim_crm_touchpoint_id=bizible_weight_snapshot.dim_crm_touchpoint_id
  LEFT JOIN attribution_touchpoint_offer_type
    ON  mart_crm_attribution_touchpoint.dim_crm_touchpoint_id=attribution_touchpoint_offer_type.dim_crm_touchpoint_id
  WHERE bizible_weight_snapshot.touchpoint_weight_snapshot_date=wk_sales_sfdc_opportunity_snapshot_history_xf_base.opportunity_snapshot_date
    OR bizible_weight_snapshot.touchpoint_weight_snapshot_date IS null 

), final AS (
  
  SELECT DISTINCT
  -- IDs
    dim_crm_opportunity_id,
    dim_crm_touchpoint_id,

  -- Dates
    created_date,
    sales_accepted_date,
    pipeline_created_date,
    close_date,
    bizible_touchpoint_date,
    touchpoint_weight_snapshot_date,
    opportunity_snapshot_date,

  -- Dimensions
    bizible_marketing_channel,
    bizible_marketing_channel_path,
    -- touchpoint_offer_type,
    -- touchpoint_offer_type_grouped,
    order_type,
    opportunity_category,
    abm_tier,
    budget_holder,
    sales_qualified_source_name,
    account_demographics_segment,
    account_demographics_geo,
    account_demographics_region,
    account_demographics_area,

  -- Sales Territory Fields
    crm_opp_owner_sales_segment_stamped,
    crm_opp_owner_geo_stamped,
    crm_opp_owner_region_stamped,
    crm_opp_owner_area_stamped,
    crm_opp_owner_business_unit_stamped,

  -- Facts
    net_arr,

  -- Flags
    is_sao,
    is_won,
    is_net_arr_pipeline_created,
    is_web_portal_purchase,
    is_edu_oss,
  
  -- Weighted Models
    bizible_count_first_touch,
    bizible_count_lead_creation_touch,
    bizible_attribution_percent_full_path,
    bizible_count_u_shaped,
    bizible_count_w_shaped,
    bizible_count_custom_model,
  
  --Weighted Net ARR
    SUM(net_arr * bizible_attribution_percent_full_path) AS bizible_net_arr_full_path,
    SUM(net_arr * bizible_count_custom_model) AS bizible_net_arr_custom_model,
    SUM(net_arr * bizible_count_first_touch) AS bizible_net_arr_first_touch,
    SUM(net_arr * bizible_count_lead_creation_touch) AS bizible_net_arr_lead_creation,
    SUM(net_arr * bizible_count_u_shaped) AS bizible_net_arr_u_shaped,
    SUM(net_arr * bizible_count_w_shaped) AS bizible_net_arr_w_shaped
  FROM combined_models
  {{dbt_utils.group_by(n=37)}}
  
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@dmicovic",
    created_date="2023-04-11",
    updated_date="2023-05-16",
  ) }}