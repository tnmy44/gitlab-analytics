WITH l2r AS (
  SELECT

   rpt_lead_to_revenue.dim_crm_person_id,
   rpt_lead_to_revenue.dim_crm_opportunity_id,
   rpt_lead_to_revenue.dim_crm_account_id,
   rpt_lead_to_revenue.dim_campaign_id,
   rpt_lead_to_revenue.dim_crm_touchpoint_id,
   rpt_lead_to_revenue.sfdc_record_id,

   rpt_lead_to_revenue.is_inquiry,
   rpt_lead_to_revenue.is_sao,
   rpt_lead_to_revenue.is_mql,
   rpt_lead_to_revenue.is_won,

   rpt_lead_to_revenue.sales_accepted_date,
   rpt_lead_to_revenue.bizible_touchpoint_date,   
   rpt_lead_to_revenue.bizible_marketing_channel_path,
   rpt_lead_to_revenue.bizible_integrated_campaign_grouping,
   rpt_lead_to_revenue.bizible_ad_campaign_name,
   rpt_lead_to_revenue.campaign_rep_role_name,
   rpt_lead_to_revenue.bizible_touchpoint_position,
   
   rpt_lead_to_revenue.inferred_employee_segment,
   rpt_lead_to_revenue.inferred_geo,
   rpt_lead_to_revenue.account_demographics_sales_segment,
   rpt_lead_to_revenue.lead_source,
   rpt_lead_to_revenue.accepted_date,
   rpt_lead_to_revenue.bizible_landing_page,
   rpt_lead_to_revenue.bizible_form_url,
   rpt_lead_to_revenue.bizible_touchpoint_type,

   rpt_lead_to_revenue.custom_sao,
   rpt_lead_to_revenue.won_custom_net_arr,
   rpt_lead_to_revenue.pipeline_custom_net_arr,
   rpt_lead_to_revenue.won_custom,


   rpt_lead_to_revenue.person_order_type,
   rpt_lead_to_revenue.opp_order_type,
   
   dim_campaign.budget_holder,
   dim_campaign.type                                            AS sfdc_campaign_type,

   fct_campaign.start_date                                      AS campaign_start_date,
   fct_campaign.region                                          AS sfdc_campaign_region,
   fct_campaign.sub_region                                      AS sfdc_campaign_sub_region,


   status AS crm_person_status,

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
   CONTAINS(rpt_lead_to_revenue.bizible_form_url_raw, 'https://gitlab.com/-/trial') AS is_trial_signup_touchpoint,

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
  FROM
  {{ ref('rpt_lead_to_revenue') }}
    LEFT JOIN {{ ref('dim_campaign') }} ON rpt_lead_to_revenue.dim_campaign_id = dim_campaign.dim_campaign_id
    LEFT JOIN {{ ref('fct_campaign') }} ON rpt_lead_to_revenue.dim_campaign_id = fct_campaign.dim_campaign_id
  WHERE
  (inferred_geo != 'JIHU' OR inferred_geo IS NULL) 
  AND ( opp_order_type IN ('1. New - First Order', '2. New - Connected', '3. Growth') OR opp_order_type IS NULL) --excludes churn and contraction opp attribution

)

  SELECT DISTINCT
    l2r.*,
    tpd.fiscal_year                     AS date_range_year,
    tpd.fiscal_quarter_name_fy          AS date_range_quarter,
    DATE_TRUNC(MONTH, tpd.date_actual)  AS date_range_month,
    tpd.first_day_of_week               AS date_range_week
  FROM
  l2r
    JOIN {{ ref('dim_date') }} tpd on l2r.bizible_touchpoint_date = tpd.date_actual
    LEFT JOIN {{ ref('dim_date') }} saod on l2r.sales_accepted_date = saod.date_actual

