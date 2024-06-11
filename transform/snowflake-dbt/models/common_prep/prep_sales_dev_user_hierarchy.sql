WITH prep_crm_opportunity AS (

    SELECT *
    FROM {{ref('prep_crm_opportunity')}}

), prep_date AS (

    SELECT *
    FROM {{ref('prep_date')}}

), prep_crm_user_daily_snapshot AS (

    SELECT *
    FROM {{ref('prep_crm_user_daily_snapshot')}}

), prep_team_member AS (

    SELECT *
    FROM {{ref('prep_team_member')}}

), sales_dev_opps AS (

    SELECT DISTINCT
        dim_crm_account_id,
        dim_crm_opportunity_id,
        created_date.date_actual AS opp_created_date,
        is_sao,
        net_arr,
        sales_accepted_date.date_actual AS sales_accepted_date,
        stage_1_discovery_date.date_actual AS stage_1_discovery_date,
        order_type,
        is_net_arr_closed_deal,
        COALESCE(opportunity_business_development_representative, opportunity_sales_development_representative) AS sdr_bdr_user_id
    FROM prep_crm_opportunity
    LEFT JOIN prep_date created_date
        ON prep_crm_opportunity.created_date_id=created_date.date_id
    LEFT JOIN prep_date sales_accepted_date
        ON prep_crm_opportunity.sales_accepted_date_id=sales_accepted_date.date_id
    LEFT JOIN prep_date stage_1_discovery_date
        ON prep_crm_opportunity.stage_1_discovery_date_id=stage_1_discovery_date.date_id
    WHERE sdr_bdr_user_id IS NOT NULL 
        AND opp_created_date >= '2023-02-01'
  
), sales_dev_hierarchy_prep AS (
  
    SELECT
        sales_dev_rep.dim_crm_user_id AS sales_dev_rep_user_id, 
        sales_dev_rep.user_role_name AS sales_dev_rep_role_name,
        sales_dev_rep.user_email AS sales_dev_rep_email,
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
        sales_dev_rep.employee_number AS sales_dev_rep_employee_number,
        sales_dev_rep.snapshot_date,
        manager.department AS sales_dev_manager_department,
        manager.user_role_name AS sales_dev_manager_user_role_name,
        manager.team AS sales_dev_manager_team,
        manager.employee_number AS sales_dev_manager_employee_number,
        manager.user_email AS sales_dev_manager_email,
        leader.department AS sales_dev_leader_department,
        leader.dim_crm_user_id AS sales_dev_leader_id,
        leader.user_name AS sales_dev_leader_name,
        leader.user_role_name AS sales_dev_leader_user_role_name,
        leader.team AS sales_dev_leader_team,
        leader.employee_number AS sales_dev_leader_employee_number,
        leader.user_email AS sales_dev_leader_email
    FROM prep_crm_user_daily_snapshot AS sales_dev_rep
    INNER JOIN sales_dev_opps
        ON sales_dev_rep.dim_crm_user_id = sales_dev_opps.sdr_bdr_user_id
    LEFT JOIN prep_crm_user_daily_snapshot AS manager
        ON sales_dev_rep.manager_id = manager.dim_crm_user_id 
            AND sales_dev_rep.snapshot_date = manager.snapshot_date
    LEFT JOIN prep_crm_user_daily_snapshot AS leader
        ON manager.manager_id = leader.dim_crm_user_id 
            AND manager.snapshot_date = leader.snapshot_date

), sales_dev_hierarchy AS (

    SELECT DISTINCT 
        sales_dev_hierarchy_prep.sales_dev_rep_user_id, 
        sales_dev_hierarchy_prep.sales_dev_rep_role_name,
        sales_dev_hierarchy_prep.sales_dev_rep_email,
        COALESCE(rep.first_name || ' ' || rep.last_name, sales_dev_hierarchy_prep.sales_dev_rep_user_name) AS sales_dev_rep_full_name,
        sales_dev_hierarchy_prep.sales_dev_rep_title,
        sales_dev_hierarchy_prep.sales_dev_rep_department,
        sales_dev_hierarchy_prep.sales_dev_rep_team,
        sales_dev_hierarchy_prep.sales_dev_rep_is_active,
        sales_dev_hierarchy_prep.crm_user_sales_segment,
        sales_dev_hierarchy_prep.crm_user_geo,
        sales_dev_hierarchy_prep.crm_user_region,
        sales_dev_hierarchy_prep.crm_user_area,
        sales_dev_hierarchy_prep.sales_dev_rep_employee_number,
        sales_dev_hierarchy_prep.snapshot_date,
        sales_dev_hierarchy_prep.sales_dev_rep_direct_manager_id,
        COALESCE(manager.first_name || ' ' || manager.last_name, sales_dev_rep_direct_manager_name) AS sales_dev_manager_full_name,
        sales_dev_hierarchy_prep.sales_dev_manager_email,
        sales_dev_hierarchy_prep.sales_dev_manager_employee_number,
        sales_dev_hierarchy_prep.sales_dev_manager_user_role_name,
        sales_dev_hierarchy_prep.sales_dev_leader_id,
        sales_dev_hierarchy_prep.sales_dev_leader_user_role_name,
        COALESCE(leader.first_name || ' ' || leader.last_name, sales_dev_hierarchy_prep.sales_dev_leader_name) AS sales_dev_leader_full_name,
        sales_dev_hierarchy_prep.sales_dev_leader_employee_number,
        sales_dev_hierarchy_prep.sales_dev_leader_email,
        CASE
        WHEN sales_dev_leader_full_name = 'Meaghan Leonard' THEN 'Meaghan Thatcher'
        WHEN sales_dev_leader_full_name = 'Jean-Baptiste Larramendy' AND sales_dev_manager_full_name = 'Brian Tabbert' THEN 'Brian Tabbert'
        WHEN sales_dev_leader_full_name = 'Jean-Baptiste Larramendy' AND sales_dev_manager_full_name = 'Elsje Smart' THEN 'Elsje Smart'
        WHEN sales_dev_leader_full_name = 'Jean-Baptiste Larramendy' AND sales_dev_manager_full_name = 'Robin Falkowski' THEN 'Robin Falkowski'
        ELSE sales_dev_leader_full_name
        END                                                            AS sales_dev_leader
    FROM sales_dev_hierarchy_prep
    LEFT JOIN prep_team_member AS rep
        ON sales_dev_rep_email = rep.work_email
    LEFT JOIN prep_team_member AS manager 
        ON sales_dev_manager_email = manager.work_email
    LEFT JOIN prep_team_member AS leader
        ON sales_dev_leader_email = leader.work_email

), final AS (

    SELECT DISTINCT 
        sales_dev_rep_employee_number,
        sales_dev_rep_role_name, 
        sales_dev_rep_full_name, 
        sales_dev_manager_employee_number,
        sales_dev_manager_user_role_name, 
        sales_dev_manager_full_name, 
        sales_dev_leader_employee_number,
        sales_dev_leader_user_role_name, 
        sales_dev_leader_full_name,
        snapshot_date
    FROM sales_dev_hierarchy 

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2024-06-11",
    updated_date="2024-06-11"
) }}