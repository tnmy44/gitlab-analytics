{{ config(alias='sfdc_opportunity_xf') }}

WITH edm_opty AS (

    SELECT  dim_crm_opportunity_id,
       dim_parent_crm_account_id,
       dim_crm_user_id,
       duplicate_opportunity_id,
       merged_crm_opportunity_id,
       record_type_id,
       ssp_id,
       dim_crm_account_id,
       opportunity_name,
       stage_name,
       reason_for_loss,
       reason_for_loss_details,
       reason_for_loss_staged,
       reason_for_loss_calc,
       risk_type,
       risk_reasons,
       downgrade_reason,
       sales_type,
       closed_buckets,
       opportunity_category,
       source_buckets,
       opportunity_sales_development_representative,
       opportunity_business_development_representative,
       opportunity_development_representative,
       sdr_or_bdr,
       iqm_submitted_by_role,
       sdr_pipeline_contribution,
       fpa_master_bookings_flag,
       sales_path,
       professional_services_value,
       primary_solution_architect,
       product_details,
       product_category,
       products_purchased,
       growth_type,
       opportunity_deal_size,
       primary_campaign_source_id,
       ga_client_id,
       deployment_preference,
       net_new_source_categories,
       invoice_number,
       opportunity_term,
       account_owner_team_stamped,
       stage_name_3plus,
       stage_name_4plus,
       stage_category,
       deal_category,
       deal_group,
       deal_size,
       calculated_deal_size,
       dr_partner_engagement,
       deal_path_engagement,
       forecast_category_name,
       opportunity_owner,
       opportunity_owner_manager,
       opportunity_owner_department,
       opportunity_owner_role,
       opportunity_owner_title,
       solutions_to_be_replaced,
       opportunity_health,
       tam_notes,
       generated_source,
       churn_contraction_type,
       churn_contraction_net_arr_bucket,
       owner_id,
       resale_partner_name,
       deal_path_name,
       order_type,
       order_type_grouped,
       order_type_live,
       dr_partner_engagement_name,
       alliance_type_name,
       alliance_type_short_name,
       channel_type_name,
       sales_qualified_source_name,
       sales_qualified_source_grouped,
       sqs_bucket_engagement,
       record_type_name,
       crm_account_name,
       parent_crm_account_name,
       account_demographics_segment,
       account_demographics_geo,
       account_demographics_region,
       account_demographics_area,
       account_demographics_territory,
       parent_crm_account_gtm_strategy,
       parent_crm_account_focus_account,
       parent_crm_account_sales_segment,
       parent_crm_account_zi_technologies,
       parent_crm_account_demographics_sales_segment,
       parent_crm_account_demographics_geo,
       parent_crm_account_demographics_region,
       parent_crm_account_demographics_area,
       parent_crm_account_demographics_territory,
       parent_crm_account_demographics_max_family_employee,
       parent_crm_account_demographics_upa_country,
       parent_crm_account_demographics_upa_state,
       parent_crm_account_demographics_upa_city,
       parent_crm_account_demographics_upa_street,
       parent_crm_account_demographics_upa_postal_code,
       crm_account_demographics_employee_count,
       crm_account_gtm_strategy,
       crm_account_focus_account,
       crm_account_zi_technologies,
       is_jihu_account,
       fy22_new_logo_target_list,
       admin_manual_source_number_of_employees,
       admin_manual_source_account_address,
       is_won,
       is_closed,
       is_edu_oss,
       is_ps_opp,
       is_sao,
       is_win_rate_calc,
       is_net_arr_pipeline_created,
       is_net_arr_closed_deal,
       is_new_logo_first_order,
       is_closed_won,
       is_web_portal_purchase,
       is_stage_1_plus,
       is_stage_3_plus,
       is_stage_4_plus,
       is_lost,
       is_open,
       is_active,
       is_risky,
       is_credit,
       is_renewal,
       is_refund,
       is_deleted,
       is_duplicate,
       is_excluded_from_pipeline_created,
       is_contract_reset,
       is_comp_new_logo_override,
       is_eligible_open_pipeline,
       is_eligible_asp_analysis,
       is_eligible_age_analysis,
       is_eligible_churn_contraction,
       is_booked_net_arr,
       is_downgrade,
       critical_deal_flag,
       sao_crm_opp_owner_stamped_name,
       sao_crm_account_owner_stamped_name,
       sao_crm_opp_owner_sales_segment_stamped,
       sao_crm_opp_owner_sales_segment_stamped_grouped,
       sao_crm_opp_owner_geo_stamped,
       sao_crm_opp_owner_region_stamped,
       sao_crm_opp_owner_area_stamped,
       sao_crm_opp_owner_segment_region_stamped_grouped,
       sao_crm_opp_owner_sales_segment_geo_region_area_stamped,
       crm_opp_owner_stamped_name,
       crm_account_owner_stamped_name,
       crm_opp_owner_sales_segment_stamped,
       crm_opp_owner_sales_segment_stamped_grouped,
       crm_opp_owner_geo_stamped,
       crm_opp_owner_region_stamped,
       crm_opp_owner_area_stamped,
       crm_opp_owner_sales_segment_region_stamped_grouped,
       crm_opp_owner_sales_segment_geo_region_area_stamped,
       crm_opp_owner_user_role_type_stamped,
       crm_user_sales_segment,
       crm_user_sales_segment_grouped,
       crm_user_geo,
       crm_user_region,
       crm_user_area,
       crm_user_sales_segment_region_grouped,
       crm_account_user_sales_segment,
       crm_account_user_sales_segment_grouped,
       crm_account_user_geo,
       crm_account_user_region,
       crm_account_user_area,
       crm_account_user_sales_segment_region_grouped,
        /*
       -- NF: 20230222 Removed to allow them to be adjusted by USER SFDC modification
       opportunity_owner_user_segment,
       opportunity_owner_user_geo,
       opportunity_owner_user_region,
       opportunity_owner_user_area,

       report_opportunity_user_segment,
       report_opportunity_user_geo,
       report_opportunity_user_region,
       report_opportunity_user_area,
       report_user_segment_geo_region_area,
       report_user_segment_geo_region_area_sqs_ot,
       key_segment,
       key_sqs,
       key_ot,
       key_segment_sqs,
       key_segment_ot,
       key_segment_geo,
       key_segment_geo_sqs,
       key_segment_geo_ot,
       key_segment_geo_region,
       key_segment_geo_region_sqs,
       key_segment_geo_region_ot,
       key_segment_geo_region_area,
       key_segment_geo_region_area_sqs,
       key_segment_geo_region_area_ot,
       key_segment_geo_area,

       sales_team_cro_level,
       sales_team_rd_asm_level,
       sales_team_vp_level,
       sales_team_avp_rd_level,
       sales_team_asm_level,       

       account_owner_team_stamped_cro_level,
       account_owner_user_segment,
       account_owner_user_geo,
       account_owner_user_region,
       account_owner_user_area,
       */

       lead_source,
       dr_partner_deal_type,
       partner_account,
       partner_account_name,
       partner_gitlab_program,
       calculated_partner_track,
       dr_status,
       distributor,
       dr_deal_id,
       dr_primary_registration,
       influence_partner,
       fulfillment_partner,
       fulfillment_partner_name,
       platform_partner,
       partner_track,
       resale_partner_track,
       is_public_sector_opp,
       is_registration_from_portal,
       calculated_discount,
       partner_discount,
       partner_discount_calc,
       comp_channel_neutral,
       count_crm_attribution_touchpoints,
       weighted_linear_iacv,
       count_campaigns,
       sa_tech_evaluation_close_status,
       sa_tech_evaluation_end_date,
       sa_tech_evaluation_start_date,
       cp_partner,
       cp_paper_process,
       cp_help,
       cp_review_notes,
       cp_champion,
       cp_close_plan,
       cp_competition,
       cp_decision_criteria,
       cp_decision_process,
       cp_economic_buyer,
       cp_identify_pain,
       cp_metrics,
       cp_risks,
       cp_value_driver,
       cp_why_do_anything_at_all,
       cp_why_gitlab,
       cp_why_now,
       cp_score,
       cp_use_cases,
       competitors,
       competitors_other_flag,
       competitors_gitlab_core_flag,
       competitors_none_flag,
       competitors_github_enterprise_flag,
       competitors_bitbucket_server_flag,
       competitors_unknown_flag,
       competitors_github_flag,
       competitors_gitlab_flag,
       competitors_jenkins_flag,
       competitors_azure_devops_flag,
       competitors_svn_flag,
       competitors_bitbucket_flag,
       competitors_atlassian_flag,
       competitors_perforce_flag,
       competitors_visual_studio_flag,
       competitors_azure_flag,
       competitors_amazon_code_commit_flag,
       competitors_circleci_flag,
       competitors_bamboo_flag,
       competitors_aws_flag,
       created_date,
       created_month,
       created_fiscal_quarter_date,
       created_fiscal_quarter_name,
       created_fiscal_year,
       sales_accepted_date,
       sales_accepted_month,
       sales_accepted_fiscal_quarter_date,
       sales_accepted_fiscal_quarter_name,
       sales_accepted_fiscal_year,
       close_date,
       close_month,
       close_fiscal_quarter_date,
       close_fiscal_quarter_name,
       close_fiscal_year,
       stage_0_pending_acceptance_date,
       stage_0_pending_acceptance_month,
       stage_0_pending_acceptance_fiscal_quarter_date,
       stage_0_pending_acceptance_fiscal_quarter_name,
       stage_0_pending_acceptance_fiscal_year,
       stage_1_discovery_date,
       stage_1_discovery_month,
       stage_1_discovery_fiscal_quarter_date,
       stage_1_discovery_fiscal_quarter_name,
       stage_1_discovery_fiscal_year,
       stage_2_scoping_date,
       stage_2_scoping_month,
       stage_2_scoping_fiscal_quarter_date,
       stage_2_scoping_fiscal_quarter_name,
       stage_2_scoping_fiscal_year,
       stage_3_technical_evaluation_date,
       stage_3_technical_evaluation_month,
       stage_3_technical_evaluation_fiscal_quarter_date,
       stage_3_technical_evaluation_fiscal_quarter_name,
       stage_3_technical_evaluation_fiscal_year,
       stage_4_proposal_date,
       stage_4_proposal_month,
       stage_4_proposal_fiscal_quarter_date,
       stage_4_proposal_fiscal_quarter_name,
       stage_4_proposal_fiscal_year,
       stage_5_negotiating_date,
       stage_5_negotiating_month,
       stage_5_negotiating_fiscal_quarter_date,
       stage_5_negotiating_fiscal_quarter_name,
       stage_5_negotiating_fiscal_year,
       stage_6_awaiting_signature_date_date,
       stage_6_awaiting_signature_date_month,
       stage_6_awaiting_signature_date_fiscal_quarter_date,
       stage_6_awaiting_signature_date_fiscal_quarter_name,
       stage_6_awaiting_signature_date_fiscal_year,
       stage_6_closed_won_date,
       stage_6_closed_won_month,
       stage_6_closed_won_fiscal_quarter_date,
       stage_6_closed_won_fiscal_quarter_name,
       stage_6_closed_won_fiscal_year,
       stage_6_closed_lost_date,
       stage_6_closed_lost_month,
       stage_6_closed_lost_fiscal_quarter_date,
       stage_6_closed_lost_fiscal_quarter_name,
       stage_6_closed_lost_fiscal_year,
       subscription_start_date,
       subscription_start_month,
       subscription_start_fiscal_quarter_date,
       subscription_start_fiscal_quarter_name,
       subscription_start_fiscal_year,
       subscription_end_date,
       subscription_end_month,
       subscription_end_fiscal_quarter_date,
       subscription_end_fiscal_quarter_name,
       subscription_end_fiscal_year,
       sales_qualified_date,
       sales_qualified_month,
       sales_qualified_fiscal_quarter_date,
       sales_qualified_fiscal_quarter_name,
       sales_qualified_fiscal_year,
       last_activity_date,
       last_activity_month,
       last_activity_fiscal_quarter_date,
       last_activity_fiscal_quarter_name,
       last_activity_fiscal_year,
       sales_last_activity_date,
       sales_last_activity_month,
       sales_last_activity_fiscal_quarter_date,
       sales_last_activity_fiscal_quarter_name,
       sales_last_activity_fiscal_year,
       technical_evaluation_date,
       technical_evaluation_month,
       technical_evaluation_fiscal_quarter_date,
       technical_evaluation_fiscal_quarter_name,
       technical_evaluation_fiscal_year,
       arr_created_date,
       arr_created_month,
       arr_created_fiscal_quarter_date,
       arr_created_fiscal_quarter_name,
       arr_created_fiscal_year,
       pipeline_created_date,
       pipeline_created_month,
       pipeline_created_fiscal_quarter_date,
       pipeline_created_fiscal_quarter_name,
       pipeline_created_fiscal_year,
       net_arr_created_date,
       net_arr_created_month,
       net_arr_created_fiscal_quarter_date,
       net_arr_created_fiscal_quarter_name,
       net_arr_created_fiscal_year,
       days_in_0_pending_acceptance,
       days_in_1_discovery,
       days_in_2_scoping,
       days_in_3_technical_evaluation,
       days_in_4_proposal,
       days_in_5_negotiating,
       days_in_sao,
       calculated_age_in_days,
       days_since_last_activity,
       arr_basis,
       iacv,
       net_iacv,
       segment_order_type_iacv_to_net_arr_ratio,
       calculated_from_ratio_net_arr,
       net_arr,
       created_and_won_same_quarter_net_arr,
       new_logo_count,
       amount,
       open_1plus_deal_count,
       open_3plus_deal_count,
       open_4plus_deal_count,
       booked_deal_count,
       churned_contraction_deal_count,
       open_1plus_net_arr,
       open_3plus_net_arr,
       open_4plus_net_arr,
       booked_net_arr,
       churned_contraction_net_arr,
       calculated_deal_count,
       booked_churned_contraction_deal_count,
       booked_churned_contraction_net_arr,
       arr,
       recurring_amount,
       true_up_amount,
       proserv_amount,
       other_non_recurring_amount,
       renewal_amount,
       total_contract_value,
       days_in_stage,
       created_by,
       updated_by,
       model_created_date,
       model_updated_date,
       dbt_updated_at,
       dbt_created_at

    --FROM prod.restricted_safe_common_mart_sales.mart_crm_opportunity
    FROM {{ref('mart_crm_opportunity')}} 

), sfdc_users_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_users_xf')}}
    --FROM prod.workspace_sales.sfdc_users_xf

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_accounts_xf')}}
    -- FROM PROD.restricted_safe_workspace_sales.sfdc_accounts_xf

), date_details AS (

    SELECT *
    FROM {{ ref('wk_sales_date_details') }}
    --FROM prod.workspace_sales.date_details

), agg_demo_keys_fy23 AS (
-- keys used for aggregated historical analysis

    SELECT *
    FROM {{ ref('wk_sales_report_agg_demo_sqs_ot_keys') }}
    --FROM restricted_safe_workspace_sales.report_agg_demo_sqs_ot_keys

), agg_demo_keys_base AS (

    SELECT *
    FROM {{ ref('wk_sales_report_agg_keys_base') }}
    --FROM restricted_safe_workspace_sales.report_agg_demo_sqs_ot_keys

), today AS (

  SELECT DISTINCT
    fiscal_year               AS current_fiscal_year,
    first_day_of_fiscal_year  AS current_fiscal_year_date
  FROM date_details
  WHERE date_actual = CURRENT_DATE

), sfdc_opportunity_xf AS (

   SELECT
    edm_opty.dbt_updated_at                         AS _last_dbt_run,
    edm_opty.dim_crm_account_id                     AS account_id,
    edm_opty.dim_crm_opportunity_id                 AS opportunity_id,
    edm_opty.opportunity_name                       AS opportunity_name,

    edm_opty.close_date                             AS close_date,
    edm_opty.created_date                           AS created_date,
    edm_opty.sales_accepted_date,
    edm_opty.sales_qualified_date,
    edm_opty.subscription_start_date                AS quote_start_date,
    edm_opty.subscription_end_date                  AS quote_end_date,

    edm_opty.days_in_stage,
    edm_opty.deployment_preference,
    edm_opty.merged_crm_opportunity_id,
    ----------------------------------------------------------
    ----------------------------------------------------------
    edm_opty.owner_id,
    -- edm_opty.opportunity_owner,
    opportunity_owner.name                          AS opportunity_owner,
    edm_opty.opportunity_owner_department,
    edm_opty.opportunity_owner_manager,
    edm_opty.opportunity_owner_role,
    edm_opty.opportunity_owner_title,
    ----------------------------------------------------------
    ----------------------------------------------------------
    edm_opty.opportunity_term,
    edm_opty.primary_campaign_source_id            AS primary_campaign_source_id,
    edm_opty.sales_path                            AS sales_path,
    edm_opty.sales_type                            AS sales_type,
    edm_opty.stage_name                            AS stage_name,
    edm_opty.order_type                            AS order_type_stamped,
    edm_opty.order_type_live                       AS order_type_live,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- Amount fields
    COALESCE(edm_opty.net_arr,0)                   AS raw_net_arr,
    edm_opty.net_arr,
    edm_opty.amount,
    edm_opty.renewal_amount,
    edm_opty.recurring_amount,
    edm_opty.true_up_amount,
    edm_opty.proserv_amount,
    edm_opty.other_non_recurring_amount,
    edm_opty.arr_basis,
    edm_opty.arr,

    edm_opty.competitors,
    edm_opty.fpa_master_bookings_flag,
    edm_opty.forecast_category_name,
    edm_opty.invoice_number,
    edm_opty.professional_services_value,
    edm_opty.reason_for_loss,
    edm_opty.reason_for_loss_details,
    edm_opty.downgrade_reason,

    edm_opty.is_downgrade,
    edm_opty.is_edu_oss,
    edm_opty.solutions_to_be_replaced,
    edm_opty.total_contract_value,
    edm_opty.is_web_portal_purchase,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- Support Team Members
    edm_opty.opportunity_business_development_representative,
    edm_opty.opportunity_sales_development_representative,
    edm_opty.opportunity_development_representative,
    -- Missing ISR & TAM

    ----------------------------------------------------------
    ----------------------------------------------------------
    edm_opty.opportunity_health,
    edm_opty.is_risky,
    edm_opty.risk_type,
    edm_opty.risk_reasons,
    edm_opty.tam_notes,
    edm_opty.days_in_1_discovery,
    edm_opty.days_in_2_scoping,
    edm_opty.days_in_3_technical_evaluation,
    edm_opty.days_in_4_proposal,
    edm_opty.days_in_5_negotiating,
    edm_opty.stage_0_pending_acceptance_date,
    edm_opty.stage_1_discovery_date,
    edm_opty.stage_2_scoping_date,
    edm_opty.stage_3_technical_evaluation_date,
    edm_opty.stage_4_proposal_date,
    edm_opty.stage_5_negotiating_date,
    edm_opty.stage_6_awaiting_signature_date_date         AS stage_6_awaiting_signature_date,
    edm_opty.stage_6_closed_won_date,
    edm_opty.stage_6_closed_lost_date,
    edm_opty.cp_champion,
    edm_opty.cp_close_plan,
    edm_opty.cp_competition,
    edm_opty.cp_decision_criteria,
    edm_opty.cp_decision_process,
    edm_opty.cp_economic_buyer,
    edm_opty.cp_identify_pain,
    edm_opty.cp_metrics,
    edm_opty.cp_risks,
    edm_opty.cp_use_cases,
    edm_opty.cp_value_driver,
    edm_opty.cp_why_do_anything_at_all,
    edm_opty.cp_why_gitlab,
    edm_opty.cp_why_now,
    edm_opty.cp_score,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- fields form opportunity source
    edm_opty.opportunity_category,
    edm_opty.product_category,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- Channel Org. fields
    edm_opty.deal_path_name                             AS deal_path,
    edm_opty.dr_partner_deal_type,
    edm_opty.dr_partner_engagement_name                 AS dr_partner_engagement, 
    edm_opty.partner_account,
    edm_opty.partner_account_name,
    edm_opty.dr_status,
    edm_opty.distributor,
    edm_opty.influence_partner,
    edm_opty.partner_track,
    edm_opty.partner_gitlab_program,
    edm_opty.is_public_sector_opp,
    edm_opty.is_registration_from_portal,
    edm_opty.calculated_discount,
    edm_opty.partner_discount,
    edm_opty.partner_discount_calc,
    edm_opty.comp_channel_neutral,
    edm_opty.fulfillment_partner                       AS resale_partner_id,
    resale_account.account_name                        AS resale_partner_name,
    edm_opty.platform_partner,

    ----------------------------------------------------------
    ----------------------------------------------------------

    -- account driven fields
    edm_opty.crm_account_name                          AS account_name,
    edm_opty.dim_parent_crm_account_id                 AS ultimate_parent_account_id,
    edm_opty.is_jihu_account,

    account_owner.user_segment               AS account_owner_user_segment,
    account_owner.user_geo                   AS account_owner_user_geo,
    account_owner.user_region                AS account_owner_user_region,
    account_owner.user_area                  AS account_owner_user_area,

    -- NF: 20230223 FY24 GTM fields, precalculated in the user object
    account_owner.business_unit         AS account_owner_user_business_unit,
    account_owner.sub_business_unit     AS account_owner_user_sub_business_unit,
    account_owner.division              AS account_owner_user_division,
    account_owner.asm                   AS account_owner_user_asm,

    edm_opty.account_demographics_segment,
    edm_opty.account_demographics_geo,
    edm_opty.account_demographics_region,
    edm_opty.account_demographics_area,
    edm_opty.account_demographics_territory,

    upa.account_demographics_sales_segment            AS upa_demographics_segment,
    upa.account_demographics_geo                      AS upa_demographics_geo,
    upa.account_demographics_region                   AS upa_demographics_region,
    upa.account_demographics_area                     AS upa_demographics_area,
    upa.account_demographics_territory                AS upa_demographics_territory,

    edm_opty.sales_qualified_source_name              AS sales_qualified_source,
    edm_opty.stage_category,
    edm_opty.calculated_partner_track,
    edm_opty.deal_path_engagement,
    edm_opty.is_refund,
    edm_opty.is_credit                                AS is_credit_flag,
    edm_opty.is_contract_reset                        AS is_contract_reset_flag,
    edm_opty.is_net_arr_pipeline_created,
    CAST(edm_opty.is_won AS INTEGER)                  AS is_won,
    edm_opty.is_lost,
    edm_opty.is_open,
    edm_opty.is_duplicate                             AS is_duplicate_flag,    
    CASE edm_opty.is_closed 
      WHEN TRUE THEN 1 
      ELSE 0 
    END                                               AS is_closed,
    edm_opty.is_closed                                AS stage_is_closed,
    edm_opty.is_active                                AS stage_is_active,
    edm_opty.is_renewal,

    -- date fields helpers -- revisit
    edm_opty.close_fiscal_quarter_name,
    edm_opty.close_fiscal_quarter_date,
    edm_opty.close_fiscal_year,
    edm_opty.close_month                                                 AS close_date_month,

    edm_opty.created_fiscal_quarter_name,
    edm_opty.created_fiscal_quarter_date,
    edm_opty.created_fiscal_year,
    edm_opty.created_month                                               AS created_date_month,

    edm_opty.subscription_start_fiscal_quarter_name                      AS quote_start_date_fiscal_quarter_name,
    edm_opty.subscription_start_fiscal_quarter_date                      AS quote_start_date_fiscal_quarter_date,
    edm_opty.subscription_start_fiscal_year                              AS quote_start_date_fiscal_year,
    edm_opty.subscription_start_month                                    AS quote_start_date_month,

    edm_opty.sales_accepted_fiscal_quarter_name,
    edm_opty.sales_accepted_fiscal_quarter_date,
    edm_opty.sales_accepted_fiscal_year,
    edm_opty.sales_accepted_month                                        AS sales_accepted_date_month,

    edm_opty.sales_qualified_fiscal_quarter_name,
    edm_opty.sales_qualified_fiscal_quarter_date,
    edm_opty.sales_qualified_fiscal_year,
    edm_opty.sales_qualified_month                                       AS sales_qualified_date_month,

    edm_opty.net_arr_created_date,
    edm_opty.net_arr_created_fiscal_quarter_name,
    edm_opty.net_arr_created_fiscal_quarter_date,
    edm_opty.net_arr_created_fiscal_year,
    edm_opty.net_arr_created_month                                       AS net_arr_created_date_month,

    edm_opty.pipeline_created_date,
    edm_opty.pipeline_created_fiscal_quarter_name,
    edm_opty.pipeline_created_fiscal_quarter_date,
    edm_opty.pipeline_created_fiscal_year,
    edm_opty.net_arr_created_month                                       AS pipeline_created_date_month,

    edm_opty.stage_1_discovery_date                                      AS stage_1_date,
    edm_opty.stage_1_discovery_month                                     AS stage_1_date_month,
    edm_opty.stage_1_discovery_fiscal_year                               AS stage_1_fiscal_year,
    edm_opty.stage_1_discovery_fiscal_quarter_name                       AS stage_1_fiscal_quarter_name,
    edm_opty.stage_1_discovery_fiscal_quarter_date                       AS stage_1_fiscal_quarter_date,

    edm_opty.stage_3_technical_evaluation_date                           AS stage_3_date,
    edm_opty.stage_3_technical_evaluation_month                          AS stage_3_date_month,
    edm_opty.stage_3_technical_evaluation_fiscal_year                    AS stage_3_fiscal_year,
    edm_opty.stage_3_technical_evaluation_fiscal_quarter_name            AS stage_3_fiscal_quarter_name,
    edm_opty.stage_3_technical_evaluation_fiscal_quarter_date            AS stage_3_fiscal_quarter_date,

    -- Last Activity Date <- This date can be in the future, it represents the date of the last activity taken or scheduled 
    edm_opty.last_activity_date,
    edm_opty.last_activity_fiscal_year,
    edm_opty.last_activity_fiscal_quarter_name,
    edm_opty.last_activity_fiscal_quarter_date,
    edm_opty.last_activity_month                                         AS last_activity_date_month,

    -- Sales Activity Date <- Last time an activity was taken against the opportunity
    edm_opty.sales_last_activity_date,
    edm_opty.sales_last_activity_fiscal_year,
    edm_opty.sales_last_activity_fiscal_quarter_name,
    edm_opty.sales_last_activity_fiscal_quarter_date,
    edm_opty.sales_last_activity_month                                   AS sales_last_activity_date_month,

    -----------------------------------------------------------------------------------------------------
    -----------------------------------------------------------------------------------------------------
    -- Opportunity User fields
    -- https://gitlab.my.salesforce.com/00N6100000ICcrD?setupid=OpportunityFields
    
    -- NF: 20230223 FY24 GTM fields, precalculated in the user object
    opportunity_owner.business_unit         AS opportunity_owner_user_business_unit,
    opportunity_owner.sub_business_unit     AS opportunity_owner_user_sub_business_unit,
    opportunity_owner.division              AS opportunity_owner_user_division,
    opportunity_owner.asm                   AS opportunity_owner_user_asm,

    opportunity_owner.user_segment          AS opportunity_owner_user_segment,
    opportunity_owner.user_geo              AS opportunity_owner_user_geo,
    opportunity_owner.user_region           AS opportunity_owner_user_region,
    opportunity_owner.user_area             AS opportunity_owner_user_area,


    edm_opty.competitors_other_flag,
    edm_opty.competitors_gitlab_core_flag,
    edm_opty.competitors_none_flag,
    edm_opty.competitors_github_enterprise_flag,
    edm_opty.competitors_bitbucket_server_flag,
    edm_opty.competitors_unknown_flag,
    edm_opty.competitors_github_flag,
    edm_opty.competitors_gitlab_flag,
    edm_opty.competitors_jenkins_flag,
    edm_opty.competitors_azure_devops_flag,
    edm_opty.competitors_svn_flag,
    edm_opty.competitors_bitbucket_flag,
    edm_opty.competitors_atlassian_flag,
    edm_opty.competitors_perforce_flag,
    edm_opty.competitors_visual_studio_flag,
    edm_opty.competitors_azure_flag,
    edm_opty.competitors_amazon_code_commit_flag,
    edm_opty.competitors_circleci_flag,
    edm_opty.competitors_bamboo_flag,
    edm_opty.competitors_aws_flag,
    edm_opty.is_comp_new_logo_override,
    edm_opty.is_stage_1_plus,
    edm_opty.is_stage_3_plus,
    edm_opty.is_stage_4_plus,
    edm_opty.stage_name_3plus,
    edm_opty.stage_name_4plus,
    edm_opty.deal_category,
    edm_opty.deal_group,
    edm_opty.calculated_deal_count                                   AS calculated_deal_count,

    ----------------------------------------------------------------
    -- NF 2022-01-28 This is probably TO BE DEPRECATED too, need to align with Channel ops
    -- PIO Flag for PIO reporting dashboard
    CASE
    WHEN edm_opty.dr_partner_engagement = 'PIO'
        THEN 1
    ELSE 0
    END                                                             AS partner_engaged_opportunity_flag,

    -- check if renewal was closed on time or not
    CASE
    WHEN LOWER(edm_opty.sales_type) like '%renewal%'
        AND start_date.first_day_of_fiscal_quarter   >= edm_opty.close_fiscal_quarter_date
        THEN 'On-Time'
    WHEN LOWER(edm_opty.sales_type) like '%renewal%'
        AND start_date.first_day_of_fiscal_quarter   < edm_opty.close_fiscal_quarter_date
        THEN 'Late'
    END                                                            AS renewal_timing_status,

    ----------------------------------------------------------------
    ----------------------------------------------------------------
    -- calculated fields for pipeline velocity report

    -- 20201021 NF: This should be replaced by a table that keeps track of excluded deals for forecasting purposes
    edm_opty.is_excluded_from_pipeline_created                     AS is_excluded_flag,
    -----------------------------------------------

    ---- measures
    edm_opty.open_1plus_deal_count,
    edm_opty.open_3plus_deal_count,
    edm_opty.open_4plus_deal_count,
    edm_opty.booked_deal_count,
    edm_opty.churned_contraction_deal_count,
    edm_opty.booked_churned_contraction_deal_count,
    edm_opty.open_1plus_net_arr,
    edm_opty.open_3plus_net_arr,
    edm_opty.open_4plus_net_arr,
    edm_opty.booked_net_arr,
    edm_opty.booked_churned_contraction_net_arr,
    edm_opty.churned_contraction_net_arr,

    -- FY23 Key fields
    -- NF: 20230213 Adjusting the segment field to try to provide closer to reality figures
   /*
    edm_opty.adjusted_report_opportunity_user_segment AS report_opportunity_user_segment,
    edm_opty.report_opportunity_user_segment          AS raw_report_opportunity_user_segment,
    edm_opty.report_opportunity_user_geo,
    edm_opty.report_opportunity_user_region,
    edm_opty.report_opportunity_user_area,
    edm_opty.report_user_segment_geo_region_area,
*/


   CASE 
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.user_segment
        ELSE opportunity_owner.user_segment
    END                                                       AS report_opportunity_user_segment,
    CASE 
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.user_geo
        ELSE opportunity_owner.user_geo
    END                                                       AS report_opportunity_user_geo,
    CASE 
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.user_region
        ELSE opportunity_owner.user_region
    END                                                       AS report_opportunity_user_region,
    CASE 
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.user_area
        ELSE opportunity_owner.user_area
    END                                                       AS report_opportunity_user_area,


    -- NF 20230214
    -- FY24 GTM calculated fields. These fields will be sourced from EDM eventually
    CASE 
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.business_unit
        ELSE opportunity_owner.business_unit
    END                                                       AS report_opportunity_user_business_unit,
    CASE 
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.sub_business_unit
        ELSE opportunity_owner.sub_business_unit
    END                                                       AS report_opportunity_user_sub_business_unit,
    CASE 
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.division
        ELSE opportunity_owner.division
    END                                                       AS report_opportunity_user_division,
    CASE 
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.asm
        ELSE opportunity_owner.asm
    END                                                       AS report_opportunity_user_asm,

    -- JK 2023-02-06 adding adjusted segment
    -- If MM / SMB and Region = META then Segment = Large
    -- If MM/SMB and Region = LATAM then Segment = Large
    -- If MM/SMB and Geo = APAC then Segment = Large
    -- Use that Adjusted Segment Field in our FY23 models
    CASE
      WHEN (report_opportunity_user_segment = 'mid-market'
            OR report_opportunity_user_segment = 'smb')
        AND report_opportunity_user_region = 'meta'
        THEN 'large'
      WHEN (report_opportunity_user_segment = 'mid-market'
            OR report_opportunity_user_segment = 'smb')
        AND report_opportunity_user_region = 'latam'
        THEN 'large'
      WHEN (report_opportunity_user_segment = 'mid-market'
            OR report_opportunity_user_segment = 'smb')
        AND report_opportunity_user_geo = 'apac'
        THEN 'large'
      ELSE report_opportunity_user_segment
    END AS adjusted_report_opportunity_user_segment,

    -- creating report_user_segment_geo_region_area_sqs_ot with adjusted segment
    LOWER(
      CONCAT(
        adjusted_report_opportunity_user_segment,
        '-',
        report_opportunity_user_geo,
        '-',
        report_opportunity_user_region,
        '-',
        report_opportunity_user_area,
        '-',
        sales_qualified_source_name,
        '-',
        order_type
      )
    ) AS report_user_segment_geo_region_area_sqs_ot,

    -- creating report_user_segment_geo_region_area with adjusted segment
    LOWER(
      CONCAT(
        adjusted_report_opportunity_user_segment,
        '-',
        report_opportunity_user_geo,
        '-',
        report_opportunity_user_region,
        '-',
        report_opportunity_user_area
      )
    ) AS report_user_segment_geo_region_area,
    
    

    -- NF 20230210 These next two fields will be eventually sourced from the EDM
    CASE
      WHEN (edm_opty.sales_qualified_source_name = 'Channel Generated' OR edm_opty.sales_qualified_source_name = 'Partner Generated')
          THEN 'Partner Sourced'
      WHEN (edm_opty.sales_qualified_source_name != 'Channel Generated' AND edm_opty.sales_qualified_source_name != 'Partner Generated')
          AND NOT LOWER(resale_account.account_name) LIKE ANY ('%google%','%gcp%','%amazon%')
          THEN 'Channel Co-Sell'
      WHEN (edm_opty.sales_qualified_source_name != 'Channel Generated' AND edm_opty.sales_qualified_source_name != 'Partner Generated')
          AND LOWER(resale_account.account_name) LIKE ANY ('%google%','%gcp%','%amazon%')
          THEN 'Alliance Co-Sell'
      ELSE 'Direct'
    END AS partner_category,

    CASE
      WHEN LOWER(resale_account.account_name) LIKE ANY ('%google%','%gcp%')
        THEN 'GCP'
      WHEN LOWER(resale_account.account_name) LIKE ANY ('%amazon%')
        THEN 'AWS'
      WHEN LOWER(resale_account.account_name) IS NOT NULL
        THEN 'Channel'
      ELSE 'Direct'
    END                                               AS alliance_partner,

    ------------------------------------------------------------------------

    LOWER(
      CONCAT(
        report_opportunity_user_business_unit,
        '-',
        report_opportunity_user_sub_business_unit,
        '-',
        report_opportunity_user_division,
        '-',
        report_opportunity_user_asm,
        '-',
        report_opportunity_user_segment,
        '-',
        report_opportunity_user_geo,
        '-',
        report_opportunity_user_region,
        '-',
        report_opportunity_user_area,
        '-',
        edm_opty.sales_qualified_source_name,
        '-',
        edm_opty.order_type,
        '-',
        opportunity_owner.role_type,
        '-',
        partner_category,
        '-',
        alliance_partner
      )
    ) AS report_bu_subbu_division_asm_user_segment_geo_region_area_sqs_ot_rt_pc_ap,


    edm_opty.deal_size,
    edm_opty.calculated_deal_size,
    edm_opty.calculated_age_in_days,
    edm_opty.is_eligible_open_pipeline              AS is_eligible_open_pipeline_flag,
    edm_opty.is_eligible_asp_analysis               AS is_eligible_asp_analysis_flag,
    edm_opty.is_eligible_age_analysis               AS is_eligible_age_analysis_flag,
    edm_opty.is_booked_net_arr                      AS is_booked_net_arr_flag,
    edm_opty.is_eligible_churn_contraction          AS is_eligible_churn_contraction_flag,
    edm_opty.created_and_won_same_quarter_net_arr,
    edm_opty.churn_contraction_net_arr_bucket,
    edm_opty.reason_for_loss_calc,    
    CASE edm_opty.is_sao 
      WHEN TRUE THEN 1 
      ELSE 0 
    END                                             AS is_eligible_sao_flag,
    edm_opty.is_deleted,
    opportunity_owner.is_rep_flag
    
    FROM edm_opty
    CROSS JOIN today
    -- Date helpers
    INNER JOIN sfdc_accounts_xf AS account
      ON account.account_id = edm_opty.dim_crm_account_id
    INNER JOIN sfdc_users_xf AS account_owner
      ON account_owner.user_id = account.owner_id
    INNER JOIN sfdc_accounts_xf AS upa
      ON upa.account_id = edm_opty.dim_parent_crm_account_id
    INNER JOIN date_details AS created_date_detail
      ON created_date_detail.date_actual = edm_opty.created_date::DATE
    INNER JOIN sfdc_users_xf AS opportunity_owner
      ON opportunity_owner.user_id = edm_opty.owner_id
    LEFT JOIN date_details AS start_date
      ON edm_opty.subscription_start_date::DATE = start_date.date_actual
    LEFT JOIN sfdc_accounts_xf AS resale_account
      ON resale_account.account_id = edm_opty.fulfillment_partner
    -- NF 20210906 remove JiHu opties from the models
    WHERE edm_opty.is_jihu_account = 0
        AND account.ultimate_parent_account_id NOT IN ('0016100001YUkWVAA1')            -- remove test account
        AND edm_opty.dim_crm_account_id NOT IN ('0014M00001kGcORQA0')                -- remove test account
        AND edm_opty.is_deleted = 0


), churn_metrics AS (

    SELECT
        o.opportunity_id,
        NVL(o.reason_for_loss, o.downgrade_reason) AS reason_for_loss_staged,
        o.reason_for_loss_details,

        CASE
          WHEN o.order_type_stamped IN ('4. Contraction','5. Churn - Partial')
            THEN 'Contraction'
          ELSE 'Churn'
        END                                    AS churn_contraction_type_calc

    FROM sfdc_opportunity_xf o
    WHERE o.order_type_stamped IN ('4. Contraction','5. Churn - Partial','6. Churn - Final')
        AND (o.is_won = 1
            OR (is_renewal = 1 AND is_lost = 1))

), oppty_final AS (

    SELECT
      sfdc_opportunity_xf.*,

      -- Customer Success related fields
      -- DRI Michael Armtz
      churn_metrics.reason_for_loss_staged,
      -- churn_metrics.reason_for_loss_calc, -- part of edm opp mart
      churn_metrics.churn_contraction_type_calc

    FROM sfdc_opportunity_xf
    CROSS JOIN today
    LEFT JOIN churn_metrics
      ON churn_metrics.opportunity_id = sfdc_opportunity_xf.opportunity_id

), add_calculated_net_arr_to_opty_final AS (

    SELECT
      oppty_final.*,


      -- JK 2023-02-06: FY23 keys for temp dashboard solution until tools are ready for FY24 keys 
      -- NF 2022-02-17 These keys are used in the pipeline metrics models and on the X-Ray dashboard to link gSheets with
      -- different aggregation levels
      LOWER(agg_demo_keys_fy23.key_sqs)                             AS key_sqs,
      LOWER(agg_demo_keys_fy23.key_ot)                              AS key_ot,
      LOWER(agg_demo_keys_fy23.key_segment)                         AS key_segment,
      LOWER(agg_demo_keys_fy23.key_segment_sqs)                     AS key_segment_sqs,
      LOWER(agg_demo_keys_fy23.key_segment_ot)                      AS key_segment_ot,
      LOWER(agg_demo_keys_fy23.key_segment_geo)                     AS key_segment_geo,
      LOWER(agg_demo_keys_fy23.key_segment_geo_sqs)                 AS key_segment_geo_sqs,
      LOWER(agg_demo_keys_fy23.key_segment_geo_ot)                  AS key_segment_geo_ot,
      LOWER(agg_demo_keys_fy23.key_segment_geo_region)              AS key_segment_geo_region,
      LOWER(agg_demo_keys_fy23.key_segment_geo_region_sqs)          AS key_segment_geo_region_sqs,
      LOWER(agg_demo_keys_fy23.key_segment_geo_region_ot)           AS key_segment_geo_region_ot,
      LOWER(agg_demo_keys_fy23.key_segment_geo_region_area)         AS key_segment_geo_region_area,
      LOWER(agg_demo_keys_fy23.key_segment_geo_region_area_sqs)     AS key_segment_geo_region_area_sqs,
      LOWER(agg_demo_keys_fy23.key_segment_geo_region_area_ot)      AS key_segment_geo_region_area_ot,
      LOWER(agg_demo_keys_fy23.key_segment_geo_area)                AS key_segment_geo_area,
      agg_demo_keys_fy23.sales_team_cro_level,
      agg_demo_keys_fy23.sales_team_rd_asm_level,
      agg_demo_keys_fy23.sales_team_vp_level,
      agg_demo_keys_fy23.sales_team_avp_rd_level,
      agg_demo_keys_fy23.sales_team_asm_level,


      -- JK 2023-02-06: FY24 keys
      LOWER(agg_demo_keys_base.business_unit)               AS business_unit,
      LOWER(agg_demo_keys_base.sub_business_unit)           AS sub_business_unit,
      LOWER(agg_demo_keys_base.division)                    AS division,
      LOWER(agg_demo_keys_base.asm)                         AS asm,

      LOWER(agg_demo_keys_base.key_bu)                      AS key_bu,
      LOWER(agg_demo_keys_base.key_bu_subbu)                AS key_bu_subbu,
      LOWER(agg_demo_keys_base.key_bu_subbu_division)       AS key_bu_subbu_division,
      LOWER(agg_demo_keys_base.key_bu_subbu_division_asm)   AS key_bu_subbu_division_asm,

      -- Created pipeline eligibility definition
      -- https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/2389
      CASE
        WHEN oppty_final.order_type_stamped IN ('1. New - First Order' ,'2. New - Connected', '3. Growth')
          AND oppty_final.is_edu_oss = 0
          AND oppty_final.pipeline_created_fiscal_quarter_date IS NOT NULL
          AND oppty_final.opportunity_category IN ('Standard','Internal Correction','Ramp Deal','Credit','Contract Reset')
          -- 20211222 Adjusted to remove the ommitted filter
          AND oppty_final.stage_name NOT IN ('00-Pre Opportunity','10-Duplicate', '9-Unqualified','0-Pending Acceptance')
          AND (net_arr > 0
            OR oppty_final.opportunity_category = 'Credit')
          -- 20220128 Updated to remove webdirect SQS deals
          AND oppty_final.sales_qualified_source  != 'Web Direct Generated'
          AND oppty_final.is_jihu_account = 0
         THEN 1
         ELSE 0
      END                                                          AS is_eligible_created_pipeline_flag


    FROM oppty_final
    -- Add keys for aggregated analysis
    LEFT JOIN agg_demo_keys_fy23
      ON oppty_final.report_user_segment_geo_region_area_sqs_ot = agg_demo_keys_fy23.report_user_segment_geo_region_area_sqs_ot
    LEFT JOIN agg_demo_keys_base
      ON oppty_final.report_bu_subbu_division_asm_user_segment_geo_region_area_sqs_ot_rt_pc_ap = agg_demo_keys_base.report_bu_subbu_division_asm_user_segment_geo_region_area_sqs_ot_rt_pc_ap


)
SELECT *
FROM add_calculated_net_arr_to_opty_final
