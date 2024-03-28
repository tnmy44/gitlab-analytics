WITH prep_marketo_person AS (

  SELECT *
  FROM {{ ref('prep_marketo_person') }} 

), final AS (

    SELECT
    -- IDs
        dim_marketo_person_id,
        {{ dbt_utils.generate_surrogate_key(['COALESCE(sfdc_contact_id, sfdc_lead_id)']) }} AS dim_crm_person_id,
        email_hash,

    -- common dimension keys
        acquisition_program_id,
        gitlabdotcom_user_id,
        dim_crm_account_id,
        demandbase_sid,

    -- person information
        title,
        country,
        sfdc_record_type,
        all_remote_function,
        all_remote_role,
        reason_for_all_remote,
        public_sector_partner,
        original_source_type,
        registration_source_type,
        tech_stack,
        original_source_info,
        marketo_person_type,
        email_bounced_reason,
        compliance_segment_value,
        marketo_person_region,
        marketo_account_region,
        preferred_language,
        marketo_account_tier,
        marketo_account_tier_notes,
        marketo_abm_tier,
        account_demographics_upa_country,
        account_demographics_upa_country_name,
        product_category,
        
    -- marketo person dimensions
        marketo_priority,
        marketo_urgency,
        marketo_relative_urgency,
        marketo_lead_source,
        marketo_lead_status,
        marketo_sales_insight,
        marketo_add_to_marketo_campaign,
        previous_nurture_reason,
        marketo_newsletter_segment,
        sec_project_names,
        email_suspended_cause,
        products_purchased,
        license_utilization,
        marketo_rating_scale,                

    -- pathfactory dimensions
        path_factory_query_string,
        path_factory_experience_name,
        path_factory_asset_type,
        path_factory_content_list,
        path_factory_funnel_state,
        path_factory_topic_list,

    -- person flags
        is_email_bounced,
        is_unsubscribed,
        is_opt_in,
        is_pql_marketo,
        is_paid_tier_marketo,
        is_ptpt_contact_marketo,
        is_ptp_contact_marketo,
        is_impacted_by_user_limit_marketo,
        is_currently_in_trial_marketo,
        is_active_user,
        is_subscribed_webcast,
        is_subscribed_live_events,
        is_gdpr_compliant,
        is_email_suspended,
        is_gs_plan_active,
        has_requested_migration_services,
        has_requested_implementation_services,
        has_requested_specialized_trainings,
        is_core_user,
        is_active_plan,
        is_secure_active,
        is_marketing_suspended,
        is_email_opt_out,
        is_events_segment,
        is_black_listed,
        is_do_not_route,
        is_double_opt_in,
        has_requested_education,
        is_30_day_opt_out,
        is_marketing_comm_opt_out,
        is_using_ce,
        is_do_not_contact,
        is_invalid_email,
        is_high_priority,
        is_unsubscribe_all_marketing
    FROM prep_marketo_person
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2024-03-28",
    updated_date="2024-03-28"
) }}