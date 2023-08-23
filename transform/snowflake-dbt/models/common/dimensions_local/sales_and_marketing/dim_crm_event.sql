ITH prep_crm_event AS (

  SELECT *
  FROM {{ ref('prep_crm_event') }}
  WHERE is_deleted = FALSE

), final AS (

    SELECT

    -- Surrogate key
        prep_crm_task.dim_crm_event_sk,

    -- Natural key
        prep_crm_task.event_id,

    -- Task infomation
        prep_crm_task.event_subject,
        prep_crm_task.event_source,
        prep_crm_task.outreach_meeting_type,
        prep_crm_task.event_type,
        prep_crm_task.event_sub_type,
        prep_crm_task.event_disposition,
        prep_crm_task.event_description, 
        prep_crm_task.duration_time_in_minutes, 
        prep_crm_task.event_subtype,
        prep_crm_task.booked_by_employee_number,
        prep_crm_task.sa_activity_type,
        prep_crm_task.event_show_as,
        prep_crm_task.assigned_to_role,
        prep_crm_task.csm_activity_type,
        prep_crm_task.customer_interaction_sentiment,
        prep_crm_task.google_doc_link,
        prep_crm_task.comments,
        prep_crm_task.qualified_convo_or_meeting,
        prep_crm_task.first_opportunity_event_created_date,
    
    --Dates and Datetimes
        prep_crm_task.event_start_date_time,
        prep_crm_task.reminder_date_time,
        prep_crm_task.event_end_date_time,
        prep_crm_task.event_date,
        prep_crm_task.event_datetime,
        prep_crm_task.created_at,
        prep_crm_task.event_end_date,


    --Event Flags
        prep_crm_task.is_all_day_event, 
        prep_crm_task.is_archived,
        prep_crm_task.is_child_event, 
        prep_crm_task.is_group_event,
        prep_crm_task.is_private_event,
        prep_crm_task.is_recurrence,
        prep_crm_task.has_reminder_set,
        prep_crm_task.is_answered,
        prep_crm_task.is_bad_number, 
        prep_crm_task.is_busy, 
        prep_crm_task.is_correct_contact,
        prep_crm_task.is_left_message,
        prep_crm_task.is_not_answered,
        prep_crm_task.is_meeting_canceled,
        prep_crm_task.is_closed_task,
        prep_crm_task.is_activity,

    --Recurrence Info
        prep_crm_task.event_recurrence_activity_id,
        prep_crm_task.event_recurrence_day_of_week,
        prep_crm_task.event_recurrence_day_of_month, 
        prep_crm_task.event_recurrence_end_date,
        prep_crm_task.event_recurrence_instance,
        prep_crm_task.event_recurrence_interval,
        prep_crm_task.event_recurrence_month_of_year, 
        prep_crm_task.event_recurrence_stat_date_time,
        prep_crm_task.event_recurrence_timezone_key,
        prep_crm_task.event_recurrence_type,
        prep_crm_task.is_recurrence_2_exclusion,
        prep_crm_task.is_recurrence_2,
        prep_crm_task.is_recurrence_2_exception,

      -- metadata
        prep_crm_task.last_modified_id,
        prep_crm_task.last_modified_date,
        prep_crm_task.systemmodstamp

    FROM prep_crm_event

    )

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-08-22",
    updated_date="2023-08-23"
) }}