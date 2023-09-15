{{ config(materialized='table') }}

{{ simple_cte([
    ('mart_crm_opportunity_stamped_hierarchy_hist','mart_crm_opportunity_stamped_hierarchy_hist'),
    ('dim_crm_user','dim_crm_user'),
    ('mart_crm_person','mart_crm_person'),
    ('sfdc_lead','sfdc_lead'),
    ('mart_crm_event','mart_crm_event'),
    ('mart_crm_task','mart_crm_task'),
    ('bdg_crm_opportunity_contact_role','bdg_crm_opportunity_contact_role')
]) }}

, sales_dev_opps AS (

  SELECT DISTINCT
    dim_crm_account_id,
    dim_crm_opportunity_id,
    net_arr,
    sales_accepted_date AS sales_accepted_date,
    created_date AS opp_created_date,
    close_date,
    pipeline_created_date,
    order_type,
    crm_opp_owner_sales_segment_stamped,
    crm_opp_owner_business_unit_stamped,
    crm_opp_owner_geo_stamped,
    crm_opp_owner_region_stamped,
    crm_opp_owner_area_stamped,
    is_sao,
    is_net_arr_closed_deal,
    is_net_arr_pipeline_created,
    is_eligible_age_analysis,
    is_eligible_open_pipeline,
    coalesce(opportunity_business_development_representative,opportunity_sales_development_representative) AS sdr_bdr_user_id
    FROM mart_crm_opportunity_stamped_hierarchy_hist
    WHERE sales_qualified_source_name = 'SDR Generated' 
      AND sales_accepted_date >= '2022-02-01' 

), sales_dev_hierarchy AS (
  
  SELECT
  --Sales Dev Data
    sales_dev_rep.dim_crm_user_id AS sales_dev_rep_user_id, 
    sales_dev_rep.user_role_name AS sales_dev_rep_role_name,
    sales_dev_rep.user_name AS sales_dev_rep_user_name,
    sales_dev_rep.title AS sales_dev_rep_title,
    sales_dev_rep.department AS sales_dev_rep_department,
    sales_dev_rep.team AS sales_dev_rep_team,
    sales_dev_rep.manager_id AS sales_dev_rep_direct_manager_id,
    sales_dev_rep.manager_name AS sales_dev_rep_direct_manager_name,
    sales_dev_rep.is_active AS sales_dev_rep_is_active,
    sales_dev_rep.crm_user_sales_segment,
    sales_dev_rep.crm_user_geo,
    sales_dev_rep.crm_user_region,
    sales_dev_rep.crm_user_area,
    sales_dev_rep.crm_user_business_unit,
  
  --Manager Data
    manager.user_role_name AS sales_dev_rep_manager_role_name,
    manager.manager_id AS sales_dev_rep_manager_id,
    manager.manager_name AS sales_dev_rep_manager_name
  FROM dim_crm_user sales_dev_rep
  INNER JOIN sales_dev_opps 
    ON sales_dev_rep.dim_crm_user_id = sales_dev_opps.sdr_bdr_user_id
  LEFT JOIN dim_crm_user manager 
    ON sales_dev_rep.manager_id = manager.dim_crm_user_id  

), merged_person_base AS (

  SELECT DISTINCT  
    mart_crm_person.dim_crm_person_id,
    sfdc_lead.converted_contact_id AS sfdc_record_id,
    sfdc_lead.lead_id AS original_lead_id,
    sales_dev_opps.DIM_CRM_OPPORTUNITY_ID,
    sales_dev_opps.opp_created_date,
    mart_crm_person.dim_crm_account_id
  FROM sfdc_lead 
  LEFT JOIN mart_crm_person 
    ON mart_crm_person.sfdc_record_id = sfdc_lead.converted_contact_id  
  LEFT JOIN sales_dev_opps 
    ON converted_opportunity_id = dim_crm_opportunity_id 
  WHERE converted_contact_id IS NOT null 

), contacts_on_opps AS (

  SELECT DISTINCT
    bdg_crm_opportunity_contact_role.sfdc_record_id AS sfdc_record_id,
    bdg_crm_opportunity_contact_role.dim_crm_person_id AS dim_crm_person_id,
    bdg_crm_opportunity_contact_role.contact_role,
    bdg_crm_opportunity_contact_role.is_primary_contact,
    sales_dev_opps.dim_crm_opportunity_id,
    sales_dev_opps.opp_created_date AS opp_created_date,
    sales_dev_opps.sales_accepted_date AS sales_accepted_date,
    sales_dev_opps.sdr_bdr_user_id AS dim_crm_user_id 
  FROM bdg_crm_opportunity_contact_role
  INNER JOIN sales_dev_opps
    ON sales_dev_opps.dim_crm_opportunity_id = bdg_crm_opportunity_contact_role.dim_crm_opportunity_id
  
), activity_base AS (

  SELECT DISTINCT
    mart_crm_event.event_id AS activity_id,
    mart_crm_event.dim_crm_user_id,
    mart_crm_event.dim_crm_opportunity_id,
    mart_crm_event.dim_crm_account_id,
    mart_crm_event.sfdc_record_id,
    mart_crm_event.dim_crm_person_id,
    dim_crm_user.dim_crm_user_id AS booked_by_user_id,
    mart_crm_event.event_date AS activity_date,
    'Event' AS activity_type,
    mart_crm_event.event_type AS activity_subtype
  FROM mart_crm_event
  LEFT JOIN dim_crm_user 
    ON booked_by_employee_number = dim_crm_user.employee_number 
  INNER JOIN sales_dev_hierarchy 
    ON mart_crm_event.dim_crm_user_id = sales_dev_hierarchy.sales_dev_rep_user_id 
      OR booked_by_user_id = sales_dev_hierarchy.sales_dev_rep_user_id 
  WHERE activity_date >= '2022-01-01'
  UNION
  SELECT 
    mart_crm_task.task_id AS activity_id,
    mart_crm_task.dim_crm_user_id,
    mart_crm_task.dim_crm_opportunity_id,
    mart_crm_task.dim_crm_account_id,
    mart_crm_task.sfdc_record_id,
    mart_crm_task.dim_crm_person_id,
    NULL AS booked_by_user_id,
    mart_crm_task.task_completed_date AS activity_date,
    mart_crm_task.task_type AS activity_type,
    mart_crm_task.task_subtype AS activity_subtype
  FROM mart_crm_task
  INNER JOIN sales_dev_hierarchy 
    ON mart_crm_task.dim_crm_user_id = sales_dev_hierarchy.sales_dev_rep_user_id
  WHERE activity_date >= '2022-01-01'

), activity_final AS (

  SELECT 
    activity_base.activity_id,
    COALESCE(activity_base.booked_by_user_id,activity_base.dim_crm_user_id) AS dim_crm_user_id,
    mart_crm_person.dim_crm_person_id,
    COALESCE(mart_crm_person.sfdc_record_id, activity_base.sfdc_record_id) AS sfdc_record_id,
    COALESCE(mart_crm_person.dim_crm_account_id, activity_base.dim_crm_account_id) AS dim_crm_account_id,
    activity_base.activity_date::DATE AS activity_date,
    activity_base.activity_type,
    activity_base.activity_subtype
  FROM activity_base
  LEFT JOIN merged_person_base 
    ON activity_base.sfdc_record_id = merged_person_base.original_lead_id
  LEFT JOIN mart_crm_person 
    ON COALESCE(merged_person_base.dim_crm_person_id,activity_base.dim_crm_person_id) = mart_crm_person.dim_crm_person_id    
  
), activity_summarised AS (

  SELECT 
    activity_final.dim_crm_user_id,
    activity_final.dim_crm_person_id,
    activity_final.dim_crm_account_id,
    activity_final.sfdc_record_id,
    activity_final.activity_date,
    activity_final.activity_type,
    activity_final.activity_subtype,
    COUNT(DISTINCT activity_id) AS tasks_completed
  FROM activity_final
  {{dbt_utils.group_by(n=7)}}

), opp_to_lead AS (

  SELECT DISTINCT
    sales_dev_opps.*,
    merged_person_base.dim_crm_person_id AS converted_person_id,
    contacts_on_opps.dim_crm_person_id AS contact_person_id,
    activity_summarised.dim_crm_person_id AS activity_person_id,
    COALESCE(merged_person_base.dim_crm_person_id,contacts_on_opps.dim_crm_person_id,activity_summarised.dim_crm_person_id) AS waterfall_person_id,
    COALESCE(DATEDIFF(DAY,activity_date,sales_dev_opps.sales_accepted_date),0) AS activity_to_sao_days
  FROM sales_dev_opps
  LEFT JOIN merged_person_base 
    ON sales_dev_opps.dim_crm_opportunity_id = merged_person_base.dim_crm_opportunity_id
  LEFT JOIN contacts_on_opps 
    ON sales_dev_opps.dim_crm_opportunity_id = contacts_on_opps.dim_crm_opportunity_id
  LEFT JOIN activity_summarised 
    ON sales_dev_opps.dim_crm_account_id = activity_summarised.dim_crm_account_id 
      AND activity_summarised.activity_date <= sales_dev_opps.sales_accepted_date 
      AND sales_dev_opps.sdr_bdr_user_id = activity_summarised.dim_crm_user_id
), opps_missing_link AS (

  SELECT * 
  FROM opp_to_lead 
  WHERE waterfall_person_id IS NULL

), final AS (

  SELECT
    mart_crm_person.dim_crm_person_id,
    mart_crm_person.sfdc_record_id,
    COALESCE(opp_to_lead.dim_crm_account_id,mart_crm_person.dim_crm_account_id) AS dim_crm_account_id,
    mart_crm_person.mql_date_latest,
    mart_crm_person.inquiry_date_pt,
    mart_crm_person.account_demographics_sales_segment AS person_sales_segment,
    mart_crm_person.account_demographics_sales_segment_grouped AS person_sales_segment_grouped,
    mart_crm_person.is_mql,
    mart_crm_person.is_first_order_person,
    CASE 
      WHEN mart_crm_person.propensity_to_purchase_score_group IS NULL 
        THEN 'No PTP Score' 
      ELSE mart_crm_person.propensity_to_purchase_score_group 
    END AS propensity_to_purchase_score_group,
    CASE 
      WHEN propensity_to_purchase_score_group = '4' OR propensity_to_purchase_score_group = '5' 
        THEN TRUE 
      ELSE FALSE 
    END AS is_high_ptp_lead,
    mart_crm_person.marketo_last_interesting_moment,
    mart_crm_person.marketo_last_interesting_moment_date,
    activity_summarised.dim_crm_user_id,
    activity_summarised.activity_date,
    activity_summarised.activity_type,
    activity_summarised.activity_subtype,
    activity_summarised.tasks_completed,
    CASE 
      WHEN activity_summarised.activity_date >= mart_crm_person.mql_date_latest 
        THEN TRUE 
      ELSE FALSE 
    END AS worked_after_mql_flag,
    opp_to_lead.dim_crm_opportunity_id,
    opp_to_lead.sdr_bdr_user_id,
    opp_to_lead.net_arr,
    opp_to_lead.sales_accepted_date,
    opp_to_lead.opp_created_date,
    opp_to_lead.close_date,
    opp_to_lead.pipeline_created_date,
    Opp_to_lead.activity_to_SAO_days,
    opp_to_lead.order_type,
    opp_to_lead.crm_opp_owner_sales_segment_stamped,
    opp_to_lead.crm_opp_owner_business_unit_stamped,
    opp_to_lead.crm_opp_owner_geo_stamped,
    opp_to_lead.crm_opp_owner_region_stamped,
    opp_to_lead.crm_opp_owner_area_stamped,
    opp_to_lead.is_sao,
    opp_to_lead.is_net_arr_closed_deal,
    opp_to_lead.is_net_arr_pipeline_created,
    opp_to_lead.is_eligible_age_analysis,
    opp_to_lead.is_eligible_open_pipeline,
    sales_dev_hierarchy.sales_dev_rep_user_id,
    sales_dev_hierarchy.sales_dev_rep_role_name,
    sales_dev_hierarchy.sales_dev_rep_user_name,
    sales_dev_hierarchy.sales_dev_rep_title,
    sales_dev_hierarchy.sales_dev_rep_department,
    sales_dev_hierarchy.sales_dev_rep_team,
    sales_dev_hierarchy.sales_dev_rep_direct_manager_id,
    sales_dev_hierarchy.sales_dev_rep_direct_manager_name,
    sales_dev_hierarchy.sales_dev_rep_is_active,
    sales_dev_hierarchy.crm_user_sales_segment,
    sales_dev_hierarchy.crm_user_geo,
    sales_dev_hierarchy.crm_user_region,
    sales_dev_hierarchy.crm_user_area,
    sales_dev_hierarchy.crm_user_business_unit,
    sales_dev_hierarchy.sales_dev_rep_manager_role_name,
    sales_dev_hierarchy.sales_dev_rep_manager_id,
    sales_dev_hierarchy.sales_dev_rep_manager_name
  FROM mart_crm_person
  LEFT JOIN activity_summarised
    ON mart_crm_person.dim_crm_person_id = activity_summarised.dim_crm_person_id 
  LEFT JOIN opp_to_lead 
    ON mart_crm_person.dim_crm_person_id = opp_to_lead.waterfall_person_id
  LEFT JOIN sales_dev_hierarchy 
  ON COALESCE(opp_to_lead.sdr_bdr_user_id,activity_summarised.dim_crm_user_id) = sales_dev_hierarchy.sales_dev_rep_user_id
  WHERE activity_to_sao_days <= 90 OR activity_to_sao_days IS NULL 
  UNION 
  SELECT
    NULL AS dim_crm_person_id,
    NULL AS sfdc_record_id,
    opps_missing_link.dim_crm_account_id AS dim_crm_account_id,
    NULL AS mql_date_latest,
    NULL AS inquiry_date_pt,
    NULL AS person_sales_segment,
    NULL AS person_sales_segment_grouped,
    NULL AS is_mql,
    NULL AS is_first_order_person,
    NULL AS propensity_to_purchase_score_group,
    NULL AS is_high_ptp_lead,
    NULL AS marketo_last_interesting_moment,
    NULL AS marketo_last_interesting_moment_date,
    NULL AS dim_crm_user_id,
    NULL AS activity_date,
    NULL AS activity_type,
    NULL AS activity_subtype,
    NULL AS tasks_completed,
    NULL AS worked_after_mql_flag,
    opps_missing_link.dim_crm_opportunity_id,
    opps_missing_link.sdr_bdr_user_id,
    opps_missing_link.net_arr,
    opps_missing_link.sales_accepted_date,
    opps_missing_link.opp_created_date,
    opps_missing_link.close_date,
    opps_missing_link.pipeline_created_date,
    opps_missing_link.activity_to_SAO_days,
    opps_missing_link.order_type,
    opps_missing_link.crm_opp_owner_sales_segment_stamped,
    opps_missing_link.crm_opp_owner_business_unit_stamped,
    opps_missing_link.crm_opp_owner_geo_stamped,
    opps_missing_link.crm_opp_owner_region_stamped,
    opps_missing_link.crm_opp_owner_area_stamped,
    opps_missing_link.is_sao,
    opps_missing_link.is_net_arr_closed_deal,
    opps_missing_link.is_net_arr_pipeline_created,
    opps_missing_link.is_eligible_age_analysis,
    opps_missing_link.is_eligible_open_pipeline,
    sales_dev_hierarchy.sales_dev_rep_user_id,
    sales_dev_hierarchy.sales_dev_rep_role_name,
    sales_dev_hierarchy.sales_dev_rep_user_name,
    sales_dev_hierarchy.sales_dev_rep_title,
    sales_dev_hierarchy.sales_dev_rep_department,
    sales_dev_hierarchy.sales_dev_rep_team,
    sales_dev_hierarchy.sales_dev_rep_direct_manager_id,
    sales_dev_hierarchy.sales_dev_rep_direct_manager_name,
    sales_dev_hierarchy.sales_dev_rep_is_active,
    sales_dev_hierarchy.crm_user_sales_segment,
    sales_dev_hierarchy.crm_user_geo,
    sales_dev_hierarchy.crm_user_region,
    sales_dev_hierarchy.crm_user_area,
    sales_dev_hierarchy.crm_user_business_unit,
    sales_dev_hierarchy.sales_dev_rep_manager_role_name,
    sales_dev_hierarchy.sales_dev_rep_manager_id,
    sales_dev_hierarchy.sales_dev_rep_manager_name
  FROM opps_missing_link
  LEFT JOIN sales_dev_hierarchy 
    ON opps_missing_link.sdr_bdr_user_id = sales_dev_hierarchy.sales_dev_rep_user_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-09-06",
    updated_date="2023-09-08",
  ) }}