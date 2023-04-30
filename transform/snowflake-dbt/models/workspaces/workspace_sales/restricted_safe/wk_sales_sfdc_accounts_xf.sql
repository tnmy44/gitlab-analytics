{{ config(alias='sfdc_accounts_xf') }}

WITH mart_crm_account AS (
    SELECT *
    FROM {{ref('mart_crm_account')}}

), account_owner AS (

SELECT *
FROM {{ref('wk_sales_sfdc_users_xf')}}

), parent_account AS (
    SELECT
        dim_crm_account_id,
        dim_crm_user_id,
        parent_crm_account_sales_segment,
        parent_crm_account_geo,
        parent_crm_account_region,
        parent_crm_account_area,
        parent_crm_account_sales_territory
    FROM {{ref('mart_crm_account')}}

), sfdc_record_type AS (
    -- using source in prep temporarily
    SELECT *
    FROM PREP.sfdc.sfdc_record_type_source
)

SELECT
    mart.dim_crm_account_id                                  AS account_id,
    mart.crm_account_name                                    AS account_name,
    mart.master_record_id,
    mart.dim_crm_user_id                                     AS owner_id,

    -----------------------

    account_owner.business_unit                 AS account_owner_user_business_unit,
    account_owner.sub_business_unit             AS account_owner_user_sub_business_unit,
    account_owner.division                      AS account_owner_user_division,
    account_owner.asm                           AS account_owner_user_asm,

    account_owner.adjusted_user_segment         AS account_owner_user_segment,

    -- NF: Add the logic for hybrid users
    -- If hybrid user we leverage the account demographics data
    CASE 
        WHEN account_owner.is_hybrid_flag = 1
        THEN mart.parent_crm_account_sales_segment  
        ELSE account_owner.user_segment  
    END                                         AS account_owner_raw_user_segment,
    mart.crm_account_owner_geo                  AS account_owner_user_geo,
    mart.crm_account_owner_region               AS account_owner_user_region,
    
    -- NF: Add the logic for hybrid users
    -- If hybrid user we leverage the account demographics data
       CASE 
        WHEN account_owner.is_hybrid_flag = 1
        THEN mart.parent_crm_account_area  
        ELSE account_owner.user_area  
       END                                      AS account_owner_user_area,

    account_owner.role_type                     AS account_owner_user_role_type,

    parent_account_owner.business_unit          AS parent_account_owner_user_business_unit,
    parent_account_owner.sub_business_unit      AS parent_account_owner_user_sub_business_unit,
    parent_account_owner.division               AS parent_account_owner_user_division,
    parent_account_owner.asm                    AS parent_account_owner_user_asm,

    parent_account_owner.role_type              AS parent_account_owner_user_role_type,

    ------------------------
    mart.dim_crm_person_primary_contact_id      AS primary_contact_id,
    mart.record_type_id,
    mart.partner_vat_tax_id,
    mart.gitlab_com_user,
    mart.account_manager,
    mart.account_owner,
    mart.business_development_rep,
    mart.dedicated_service_engineer,
    mart.sales_development_rep,
    mart.technical_account_manager_id,
    mart.dim_parent_crm_account_id                           AS ultimate_parent_account_id,
    mart.crm_account_type                                    AS account_type,
    mart.crm_account_industry                                AS industry,
    mart.crm_account_sub_industry                            AS sub_industry,
    mart.parent_crm_account_industry                         AS parent_account_industry,
    mart.account_tier,
    mart.customer_since_date,
    mart.carr_this_account,
    mart.carr_account_family,
    mart.next_renewal_date,
    mart.license_utilization,
    mart.support_level,
    mart.named_account,
    mart.crm_account_billing_country                         AS billing_country,
    mart.crm_account_billing_country_code                    AS billing_country_code,
    mart.billing_postal_code,
    mart.is_sdr_target_account,
    mart.parent_crm_account_lam                              AS lam,
    mart.parent_crm_account_lam_dev_count                    AS lam_dev_count,
    mart.is_jihu_account,
    mart.partners_signed_contract_date,
    mart.partner_account_iban_number,
    mart.partner_type,
    mart.partner_status,
    mart.is_first_order_available,
    mart.crm_account_zi_technologies                         AS zi_technologies,
    mart.technical_account_manager_date,
    mart.gitlab_customer_success_project,
    mart.forbes_2000_rank,
    mart.potential_users,
    mart.number_of_licenses_this_account,
    mart.decision_maker_count_linkedin,
    mart.number_of_employees,
    mart.account_phone,
    mart.zoominfo_account_phone,
    mart.admin_manual_source_number_of_employees,
    mart.admin_manual_source_account_address,
    mart.parent_crm_account_sales_segment       AS account_demographics_sales_segment,
    mart.parent_crm_account_geo                 AS account_demographics_geo,
    mart.parent_crm_account_region              AS account_demographics_region,
    mart.parent_crm_account_area                AS account_demographics_area,
    mart.parent_crm_account_sales_territory     AS account_demographics_territory,
    mart.crm_account_employee_count             AS account_demographics_employee_count,
    mart.parent_crm_account_max_family_employee AS account_demographics_max_family_employee,
    mart.parent_crm_account_upa_country         AS account_demographics_upa_country,
    mart.parent_crm_account_upa_state           AS account_demographics_upa_state,
    mart.parent_crm_account_upa_city            AS account_demographics_upa_city,
    mart.parent_crm_account_upa_street          AS account_demographics_upa_street,
    mart.parent_crm_account_upa_postal_code     AS account_demographics_upa_postal_code,
    mart.health_number,
    mart.health_score_color,
    mart.count_active_subscription_charges,
    mart.count_active_subscriptions,
    mart.count_billing_accounts,
    mart.count_licensed_users,
    mart.count_of_new_business_won_opportunities,
    mart.count_open_renewal_opportunities,
    mart.count_opportunities,
    mart.count_products_purchased,
    mart.count_won_opportunities,
    mart.count_concurrent_ee_subscriptions,
    mart.count_ce_instances,
    mart.count_active_ce_users,
    mart.count_open_opportunities,
    mart.count_using_ce,
    mart.abm_tier,
    mart.crm_account_gtm_strategy                             AS gtm_strategy,
    mart.gtm_acceleration_date,
    mart.gtm_account_based_date,
    mart.gtm_account_centric_date,
    mart.abm_tier_1_date,
    mart.abm_tier_2_date,
    mart.abm_tier_3_date,
    mart.demandbase_account_list,
    mart.demandbase_intent,
    mart.demandbase_page_views,
    mart.demandbase_score,
    mart.demandbase_sessions,
    mart.demandbase_trending_offsite_intent,
    mart.demandbase_trending_onsite_engagement,
    mart.is_locally_managed_account,
    mart.is_strategic_account,
    mart.partner_track,
    mart.partners_partner_type,
    mart.gitlab_partner_program,
    mart.zoom_info_company_name,
    mart.zoom_info_company_revenue,
    mart.zoom_info_company_employee_count,
    mart.zoom_info_company_industry,
    mart.zoom_info_company_city,
    mart.zoom_info_company_state_province,
    mart.zoom_info_company_country,
    mart.is_excluded_from_zoom_info_enrich,
    mart.crm_account_zoom_info_website                              AS zoom_info_website,
    mart.crm_account_zoom_info_company_other_domains                AS zoom_info_company_other_domains,
    mart.crm_account_zoom_info_dozisf_zi_id                         AS zoom_info_dozisf_zi_id,
    mart.crm_account_zoom_info_parent_company_zi_id                 AS zoom_info_parent_company_zi_id,
    mart.crm_account_zoom_info_parent_company_name                  AS zoom_info_parent_company_name,
    mart.crm_account_zoom_info_ultimate_parent_company_zi_id        AS zoom_info_ultimate_parent_company_zi_id,
    mart.crm_account_zoom_info_ultimate_parent_company_name         AS zoom_info_ultimate_parent_company_name,
    mart.crm_account_zoom_info_number_of_developers                 AS zoom_info_number_of_developers,
    mart.crm_account_zoom_info_total_funding                        AS zoom_info_total_funding,
    mart.is_key_account,
    mart.created_by_id,
    mart.crm_account_created_date                                   AS created_date,
    mart.is_deleted,
    mart.last_modified_by_id,
    mart.last_modified_date,
    mart.last_activity_date,
    mart.dbt_updated_at                                             AS _last_dbt_run,
    mart.technical_account_manager,
    mart.parent_crm_account_name                                    AS ultimate_parent_account_name,

    sfdc_record_type.record_type_name,
    sfdc_record_type.business_process_id,
    sfdc_record_type.record_type_label,
    sfdc_record_type.record_type_description,
    sfdc_record_type.record_type_modifying_object_type,

    mart.is_zi_jenkins_present                                      AS zi_jenkins_presence_flag,
    mart.is_zi_svn_present                                          AS zi_svn_presence_flag,
    mart.is_zi_tortoise_svn_present                                 AS zi_tortoise_svn_presence_flag,
    mart.is_zi_gcp_present                                          AS zi_gcp_presence_flag,
    mart.is_zi_atlassian_present                                    AS zi_atlassian_presence_flag,
    mart.is_zi_github_present                                       AS zi_github_presence_flag,
    mart.is_zi_github_enterprise_present                            AS zi_github_enterprise_presence_flag,
    mart.is_zi_aws_present                                          AS zi_aws_presence_flag,
    mart.is_zi_kubernetes_present                                   AS zi_kubernetes_presence_flag,
    mart.is_zi_apache_subversion_present                            AS zi_apache_subversion_presence_flag,
    mart.is_zi_apache_subversion_svn_present                        AS zi_apache_subversion_svn_presence_flag,
    mart.is_zi_hashicorp_present                                    AS zi_hashicorp_presence_flag,
    mart.is_zi_aws_cloud_trail_present                              AS zi_aws_cloud_trail_presence_flag,
    mart.is_zi_circle_ci_present                                    AS zi_circle_ci_presence_flag,
    mart.is_zi_bit_bucket_present                                   AS zi_bit_bucket_presence_flag,

 

    parent_account.parent_crm_account_sales_segment                 AS upa_segment,
    parent_account.parent_crm_account_geo                           AS upa_geo,
    parent_account.parent_crm_account_region                        AS upa_region,
    parent_account.parent_crm_account_area                          AS upa_area,
    parent_account.parent_crm_account_sales_territory                     AS upa_territory


FROM mart_crm_account AS mart
LEFT JOIN parent_account
    ON mart.dim_parent_crm_account_id = parent_account.dim_crm_account_id
LEFT JOIN sfdc_record_type
    ON mart.record_type_id = sfdc_record_type.record_type_id
LEFT JOIN account_owner 
    ON account_owner.user_id = mart.dim_crm_user_id
LEFT JOIN account_owner parent_account_owner
    ON parent_account_owner.user_id = parent_account.dim_crm_user_id

WHERE mart.is_deleted = FALSE