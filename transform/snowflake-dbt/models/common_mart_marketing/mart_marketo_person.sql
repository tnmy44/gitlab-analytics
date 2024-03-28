{{ simple_cte([
    ('dim_marketo_person','dim_marketo_person'),
    ('fct_marketo_person','fct_marketo_person')
]) }}

, final AS (

    SELECT
     -- IDs
        dim_marketo_person.dim_marketo_person_id,
        dim_marketo_person.dim_crm_person_id,
        dim_marketo_person.email_hash,

    -- common dimension keys
        dim_marketo_person.acquisition_program_id,
        dim_marketo_person.gitlabdotcom_user_id,
        dim_marketo_person.dim_crm_account_id,
        dim_marketo_person.demandbase_sid,

    -- important person dates
        fct_marketo_person.marketo_email_bounced_date,
        fct_marketo_person.marketo_acquisition_date,
        fct_marketo_person.marketo_opt_in_date,
        fct_marketo_person.marketo_created_date_time,
        fct_marketo_person.sign_up_date,
        fct_marketo_person.opt_out_date,
        fct_marketo_person.email_suspended_date,
        fct_marketo_person.initial_start_date,
        fct_marketo_person.customer_health_score_date,
        fct_marketo_person.double_opt_in_date,
        fct_marketo_person.path_factory_engagement_time,

    -- person information
        dim_marketo_person.title,
        dim_marketo_person.country,
        dim_marketo_person.sfdc_record_type,
        dim_marketo_person.all_remote_function,
        dim_marketo_person.all_remote_role,
        dim_marketo_person.reason_for_all_remote,
        dim_marketo_person.public_sector_partner,
        dim_marketo_person.original_source_type,
        dim_marketo_person.registration_source_type,
        dim_marketo_person.tech_stack,
        dim_marketo_person.original_source_info,
        dim_marketo_person.marketo_person_type,
        dim_marketo_person.email_bounced_reason,
        dim_marketo_person.compliance_segment_value,
        dim_marketo_person.marketo_person_region,
        dim_marketo_person.marketo_account_region,
        dim_marketo_person.preferred_language,
        dim_marketo_person.marketo_account_tier,
        dim_marketo_person.marketo_account_tier_notes,
        dim_marketo_person.marketo_abm_tier,
        dim_marketo_person.account_demographics_upa_country,
        dim_marketo_person.account_demographics_upa_country_name,
        dim_marketo_person.product_category,
        
    -- marketo person dimensions
        dim_marketo_person.marketo_priority,
        dim_marketo_person.marketo_urgency,
        dim_marketo_person.marketo_relative_urgency,
        dim_marketo_person.marketo_lead_source,
        dim_marketo_person.marketo_lead_status,
        dim_marketo_person.marketo_sales_insight,
        dim_marketo_person.marketo_add_to_marketo_campaign,
        dim_marketo_person.previous_nurture_reason,
        dim_marketo_person.marketo_newsletter_segment,
        dim_marketo_person.sec_project_names,
        dim_marketo_person.email_suspended_cause,
        dim_marketo_person.products_purchased,
        dim_marketo_person.license_utilization,
        dim_marketo_person.marketo_rating_scale,                

    -- pathfactory dimensions
        dim_marketo_person.path_factory_query_string,
        dim_marketo_person.path_factory_experience_name,
        dim_marketo_person.path_factory_asset_type,
        dim_marketo_person.path_factory_content_list,
        dim_marketo_person.path_factory_funnel_state,
        dim_marketo_person.path_factory_topic_list,

    -- person flags
        dim_marketo_person.is_email_bounced,
        dim_marketo_person.is_unsubscribed,
        dim_marketo_person.is_opt_in,
        dim_marketo_person.is_pql_marketo,
        dim_marketo_person.is_paid_tier_marketo,
        dim_marketo_person.is_ptpt_contact_marketo,
        dim_marketo_person.is_ptp_contact_marketo,
        dim_marketo_person.is_impacted_by_user_limit_marketo,
        dim_marketo_person.is_currently_in_trial_marketo,
        dim_marketo_person.is_active_user,
        dim_marketo_person.is_subscribed_webcast,
        dim_marketo_person.is_subscribed_live_events,
        dim_marketo_person.is_gdpr_compliant,
        dim_marketo_person.is_email_suspended,
        dim_marketo_person.is_gs_plan_active,
        dim_marketo_person.has_requested_migration_services,
        dim_marketo_person.has_requested_implementation_services,
        dim_marketo_person.has_requested_specialized_trainings,
        dim_marketo_person.is_core_user,
        dim_marketo_person.is_active_plan,
        dim_marketo_person.is_secure_active,
        dim_marketo_person.is_marketing_suspended,
        dim_marketo_person.is_email_opt_out,
        dim_marketo_person.is_events_segment,
        dim_marketo_person.is_black_listed,
        dim_marketo_person.is_do_not_route,
        dim_marketo_person.is_double_opt_in,
        dim_marketo_person.has_requested_education,
        dim_marketo_person.is_30_day_opt_out,
        dim_marketo_person.is_marketing_comm_opt_out,
        dim_marketo_person.is_using_ce,
        dim_marketo_person.is_do_not_contact,
        dim_marketo_person.is_invalid_email,
        dim_marketo_person.is_high_priority,
        dim_marketo_person.is_unsubscribe_all_marketing,

    -- additive fields
        fct_marketo_person.marketo_relative_person_score,
        fct_marketo_person.marketo_demographic_score,
        fct_marketo_person.path_factory_engagement_score,
        fct_marketo_person.path_factory_assets_viewed_count,
        fct_marketo_person.path_factory_content_count,
        fct_marketo_person.marketo_behavior_score,
        fct_marketo_person.license_user_count,
        fct_marketo_person.marketo_health_score,
        fct_marketo_person.engagio_pipeline_predict_score,
        fct_marketo_person.infer_score,
        fct_marketo_person.data_quality_score,
        fct_marketo_person.marketo_lead_score_2

    FROM dim_marketo_person
    LEFT JOIN fct_marketo_person
        ON dim_marketo_person.dim_marketo_person_id=fct_marketo_person.dim_marketo_person_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2024-03-28",
    updated_date="2024-03-28"
) }}