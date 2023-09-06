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
    created_date as opp_created_date,
    is_sao,
    net_arr,
    sales_accepted_date as sales_accepted_date,
    coalesce(opportunity_business_development_representative,opportunity_sales_development_representative) AS sdr_bdr_user_id
  FROM mart_crm_opportunity_stamped_hierarchy_hist
  WHERE sales_qualified_source_name = 'SDR Generated' 
    AND created_date >= '2023-02-01' 

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
    converted_contact_id AS sfdc_record_id,
    lead_id AS original_lead_id,
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
    opportunity.dim_crm_opportunity_id,
    opportunity.opp_created_date AS opp_created_date,
    opportunity.sales_accepted_date AS sales_accepted_date,
    opportunity.sdr_bdr_user_id AS dim_crm_user_id 
  FROM bdg_crm_opportunity_contact_role
  INNER JOIN sales_dev_opps opportunity
    ON opportunity.dim_crm_opportunity_id = bdg_crm_opportunity_contact_role.dim_crm_opportunity_id
  
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
    event_type AS activity_subtype
  FROM mart_crm_event
  LEFT JOIN dim_crm_user 
    ON booked_by_employee_number = dim_crm_user.employee_number 
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
    task_type AS activity_type,
    task_subtype AS activity_subtype
  FROM mart_crm_task

), activity_final AS (
  
  SELECT 
    activity_id,
    activity_base.dim_crm_user_id,
    activity_base.booked_by_user_id,
    mart_crm_person.dim_crm_person_id,
    COALESCE(mart_crm_person.sfdc_record_id, activity_base.sfdc_record_id) AS sfdc_record_id,
    COALESCE(mart_crm_person.dim_crm_account_id, activity_base.dim_crm_account_id) AS dim_crm_account_id,
    activity_date,
    activity_type,
    activity_subtype
  FROM activity_base
  LEFT JOIN merged_person_base 
    ON activity_base.sfdc_record_id = merged_person_base.original_lead_id
  LEFT JOIN mart_crm_person 
    ON COALESCE(merged_person_base.dim_crm_person_id,activity_base.dim_crm_person_id) = mart_crm_person.dim_crm_person_id 
   
), final AS (

  SELECT 
    sales_dev_opps.*,
    merged_person_base.dim_crm_person_id AS converted_person_id,
    contacts_on_opps.dim_crm_person_id AS contact_person_id,
    activity_final.dim_crm_person_id AS activity_person_id,
    COALESCE(merged_person_base.dim_crm_person_id,contacts_on_opps.dim_crm_person_id,activity_final.dim_crm_person_id) AS waterfall_person_id,
    COALESCE(DATEDIFF(DAY, activity_date,sales_dev_opps.sales_accepted_date),0) AS activity_to_sao_days
  FROM sales_dev_opps
  LEFT JOIN merged_person_base 
    ON sales_dev_opps.dim_crm_opportunity_id = merged_person_base.dim_crm_opportunity_id
  LEFT JOIN contacts_on_opps 
    ON sales_dev_opps.dim_crm_opportunity_id = contacts_on_opps.dim_crm_opportunity_id
  LEFT JOIN activity_final
    ON sales_dev_opps.dim_crm_account_id = activity_final.dim_crm_account_id 
      AND activity_final.activity_date <= sales_dev_opps.sales_accepted_date 
      AND sales_dev_opps.sdr_bdr_user_id = activity_final.dim_crm_user_id
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-09-06",
    updated_date="2023-09-06",
  ) }}
