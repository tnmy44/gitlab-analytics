{{ config(materialized='table') }}

{{ simple_cte([
    ('wk_marketing_rpt_lead_to_revenue_revised','wk_marketing_rpt_lead_to_revenue_revised'),
    ('dim_campaign','dim_campaign'),
    ('fct_campaign','fct_campaign'), 
    ('dim_date','dim_date')
]) }}

, l2r AS (
    
  SELECT

   wk_marketing_rpt_lead_to_revenue_revised.dim_crm_person_id,
   wk_marketing_rpt_lead_to_revenue_revised.dim_crm_opportunity_id,
   wk_marketing_rpt_lead_to_revenue_revised.dim_crm_account_id,
   wk_marketing_rpt_lead_to_revenue_revised.dim_campaign_id,
   wk_marketing_rpt_lead_to_revenue_revised.dim_crm_touchpoint_id,
   wk_marketing_rpt_lead_to_revenue_revised.sfdc_record_id,

   wk_marketing_rpt_lead_to_revenue_revised.is_inquiry,
   wk_marketing_rpt_lead_to_revenue_revised.is_sao,
   wk_marketing_rpt_lead_to_revenue_revised.is_mql,
   wk_marketing_rpt_lead_to_revenue_revised.is_won AS is_closed_won,

   wk_marketing_rpt_lead_to_revenue_revised.sales_accepted_date,
   wk_marketing_rpt_lead_to_revenue_revised.bizible_touchpoint_date,   
   wk_marketing_rpt_lead_to_revenue_revised.bizible_marketing_channel_path,
   wk_marketing_rpt_lead_to_revenue_revised.bizible_integrated_campaign_grouping,
   wk_marketing_rpt_lead_to_revenue_revised.bizible_ad_campaign_name,
   wk_marketing_rpt_lead_to_revenue_revised.campaign_rep_role_name,
   wk_marketing_rpt_lead_to_revenue_revised.bizible_touchpoint_position,
   
   wk_marketing_rpt_lead_to_revenue_revised.inferred_employee_segment,
   wk_marketing_rpt_lead_to_revenue_revised.inferred_geo,
   wk_marketing_rpt_lead_to_revenue_revised.account_demographics_sales_segment,
   wk_marketing_rpt_lead_to_revenue_revised.lead_source,
   wk_marketing_rpt_lead_to_revenue_revised.accepted_date,
   wk_marketing_rpt_lead_to_revenue_revised.bizible_landing_page,
   wk_marketing_rpt_lead_to_revenue_revised.bizible_form_url,
   wk_marketing_rpt_lead_to_revenue_revised.bizible_touchpoint_type AS touchpoint_type,

   wk_marketing_rpt_lead_to_revenue_revised.custom_sao,
   wk_marketing_rpt_lead_to_revenue_revised.won_custom_net_arr,
   wk_marketing_rpt_lead_to_revenue_revised.pipeline_custom_net_arr,
   wk_marketing_rpt_lead_to_revenue_revised.won_custom,


   rpt_lead_to_revenue.person_order_type,
   rpt_lead_to_revenue.opp_order_type,
   
   dim_campaign.budget_holder,
   dim_campaign.type                                            AS sfdc_campaign_type,

   fct_campaign.start_date                                      AS campaign_start_date,
   fct_campaign.region                                          AS sfdc_campaign_region,
   fct_campaign.sub_region                                      AS sfdc_campaign_sub_region,


   crm_person_status,

   --UTMs not captured by the Bizible
   PARSE_URL(bizible_form_url_raw):parameters:utm_content       AS bizible_form_page_utm_content,
   PARSE_URL(bizible_form_url_raw):parameters:utm_budget        AS bizible_form_page_utm_budget,
   PARSE_URL(bizible_form_url_raw):parameters:utm_allptnr       AS bizible_form_page_utm_allptnr,
   PARSE_URL(bizible_form_url_raw):parameters:utm_partnerid     AS bizible_form_page_utm_partnerid,

   PARSE_URL(bizible_landing_page_raw):parameters:utm_content   AS bizible_landing_page_utm_content,
   PARSE_URL(bizible_landing_page_raw):parameters:utm_budget    AS bizible_landing_page_utm_budget,
   PARSE_URL(bizible_landing_page_raw):parameters:utm_allptnr   AS bizible_landing_page_utm_allptnr,
   PARSE_URL(bizible_landing_page_raw):parameters:utm_partnerid AS bizible_landing_page_utm_partnerid,

   COALESCE(bizible_landing_page_utm_budget, bizible_form_page_utm_budget)       AS utm_budget,
   COALESCE(bizible_landing_page_utm_content, bizible_form_page_utm_content)     AS utm_content,
   COALESCE(bizible_landing_page_utm_allptnr, bizible_form_page_utm_allptnr)     AS utm_allptnr,
   COALESCE(bizible_landing_page_utm_partnerid, bizible_form_page_utm_partnerid) AS utm_partnerid,
   CONTAINS(wk_marketing_rpt_lead_to_revenue_revised.bizible_form_url_raw, 'https://gitlab.com/-/trial') AS is_trial_signup_touchpoint,

   fct_campaign.budgeted_cost,
   fct_campaign.actual_cost,

   CASE WHEN (LOWER(utm_content) LIKE '%field%'
             OR campaign_rep_role_name LIKE '%Field Marketing%'
             OR budget_holder = 'fmm'
             OR utm_budget = 'fmm') 
             THEN 'Field Marketing'
        WHEN (LOWER(utm_content) LIKE '%abm%'
             OR campaign_rep_role_name LIKE '%ABM%'
             OR budget_holder = 'abm'
             OR utm_budget = 'abm')
             THEN 'Account Based Marketing'
        WHEN (lower(utm_budget) LIKE '%ptnr%' 
             OR lower(utm_budget) LIKE '%chnl%')
             OR (lower(budget_holder) LIKE '%ptnr%' 
             OR lower(budget_holder) LIKE '%chnl%')
             THEN 'Partner Marketing'
        WHEN (lower(budget_holder) LIKE '%corp%' 
             OR lower(utm_budget) LIKE '%corp%')
             THEN 'Corporate Events'
        WHEN (lower(budget_holder) LIKE '%dmp%' 
             OR lower(utm_budget) LIKE '%dmp%')
             THEN 'Digital Marketing'
        ELSE 'No Budget Holder' END AS intergrated_budget_holder
  FROM wk_marketing_rpt_lead_to_revenue_revised
    LEFT JOIN dim_campaign ON wk_marketing_rpt_lead_to_revenue_revised.dim_campaign_id = dim_campaign.dim_campaign_id
    LEFT JOIN fct_campaign ON wk_marketing_rpt_lead_to_revenue_revised.dim_campaign_id = fct_campaign.dim_campaign_id
  WHERE
  (inferred_geo != 'JIHU' OR inferred_geo IS NULL) 
  AND ( opp_order_type IN ('1. New - First Order', '2. New - Connected', '3. Growth') OR opp_order_type IS NULL) --excludes churn and contraction opp attribution

), final AS (

  SELECT
    l2r.*,
    tpd.fiscal_year                     AS date_range_year,
    tpd.fiscal_quarter_name_fy          AS date_range_quarter,
    DATE_TRUNC(MONTH, tpd.date_actual)  AS date_range_month,
    tpd.first_day_of_week               AS date_range_week
  FROM
  l2r
    JOIN dim_date tpd on l2r.bizible_touchpoint_date = tpd.date_actual
    LEFT JOIN dim_date saod on l2r.sales_accepted_date = saod.date_actual

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-05-18",
    updated_date="2023-05-18",
  ) }}