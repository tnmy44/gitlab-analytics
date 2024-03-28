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

    -- important person dates
        marketo_email_bounced_date,
        marketo_acquisition_date,
        marketo_opt_in_date,
        marketo_created_date_time,
        sign_up_date,
        opt_out_date,
        email_suspended_date,
        initial_start_date,
        customer_health_score_date,
        double_opt_in_date,
        path_factory_engagement_time,

    -- additive fields
        marketo_relative_person_score,
        marketo_demographic_score,
        path_factory_engagement_score,
        path_factory_assets_viewed_count,
        path_factory_content_count,
        marketo_behavior_score,
        license_user_count,
        marketo_health_score,
        engagio_pipeline_predict_score,
        infer_score,
        data_quality_score,
        marketo_lead_score_2
    FROM prep_marketo_person
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2024-03-28",
    updated_date="2024-03-28"
) }}