{{ simple_cte([
    ('prep_crm_person', 'prep_crm_person'),
    ('sfdc_lead_source','sfdc_lead_source')
    ]) }}

, prep_crm_task AS (

  SELECT *
  FROM {{ ref('prep_crm_task') }} 
  WHERE is_deleted = FALSE

  ), converted_leads AS (

  SELECT
    sfdc_lead_source.converted_contact_id AS sfdc_record_id,
    sfdc_lead_source.lead_id,
    prep_crm_person.dim_crm_person_id
  FROM sfdc_lead_source
  LEFT JOIN prep_crm_person
    ON sfdc_lead_source.converted_contact_id=prep_crm_person.sfdc_record_id
  WHERE sfdc_lead_source.is_converted = TRUE

), final AS (

  SELECT
    -- Primary key
    prep_crm_task.dim_crm_task_pk,

    -- Foreign keys
    {{ get_keyed_nulls('prep_crm_task.dim_crm_task_sk') }}        AS dim_crm_task_sk,
    {{ get_keyed_nulls('prep_crm_task.dim_crm_account_id') }}     AS dim_crm_account_id,
    {{ get_keyed_nulls('prep_crm_task.dim_crm_user_id') }}        AS dim_crm_user_id,
    {{ get_keyed_nulls('prep_crm_task.sfdc_record_type_id') }}    AS sfdc_record_type_id,
    {{ get_keyed_nulls('prep_crm_person.dim_crm_person_id') }}    AS dim_crm_person_id,
    {{ get_keyed_nulls('prep_crm_task.dim_crm_opportunity_id') }} AS dim_crm_opportunity_id,
    COALESCE(converted_leads.sfdc_record_id,prep_crm_task.sfdc_record_id) 
                                                                  AS sfdc_record_id,

    -- Dates
    {{ get_date_id('prep_crm_task.task_date') }}                  AS task_date_id,
    prep_crm_task.task_date,
    {{ get_date_id('prep_crm_task.task_completed_date') }}        AS task_completed_date_id,
    prep_crm_task.task_completed_date,
    {{ get_date_id('prep_crm_task.reminder_date') }}              AS reminder_date_id,
    prep_crm_task.reminder_date,
    {{ get_date_id('prep_crm_task.task_recurrence_date') }}       AS task_recurrence_date_id,
    prep_crm_task.task_recurrence_date,
    {{ get_date_id('prep_crm_task.task_recurrence_start_date') }} AS task_recurrence_start_date_id,
    prep_crm_task.task_recurrence_start_date,

    -- Counts
    prep_crm_task.account_or_opportunity_count,
    prep_crm_task.lead_or_contact_count,

    -- Task durations
    prep_crm_task.hours_waiting_before_task,
    prep_crm_task.hours_waiting_before_email_task,
    prep_crm_task.call_task_duration_in_seconds,
    prep_crm_task.hours_waiting_before_call_task,

    -- Metadata
    prep_crm_task.task_created_by_id,
    {{ get_date_id('prep_crm_task.task_created_date') }}          AS task_created_date_id,
    prep_crm_task.task_created_date,
    prep_crm_task.last_modified_id,
    {{ get_date_id('prep_crm_task.last_modified_date') }}         AS last_modified_date_id,
    prep_crm_task.last_modified_date
  FROM prep_crm_task
  LEFT JOIN converted_leads
    ON prep_crm_task.sfdc_record_id=converted_leads.lead_id
  LEFT JOIN prep_crm_person
    ON COALESCE(converted_leads.sfdc_record_id,prep_crm_task.sfdc_record_id) = prep_crm_person.sfdc_record_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@rkohnke",
    created_date="2022-12-05",
    updated_date="2023-08-24"
) }}