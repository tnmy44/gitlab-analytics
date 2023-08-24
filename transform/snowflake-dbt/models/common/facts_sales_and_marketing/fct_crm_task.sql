{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('prep_crm_person', 'prep_crm_person'),
    ('dim_date', 'dim_date')
]) }}

, prep_crm_task AS (

  SELECT *
  FROM {{ ref('prep_crm_task') }}
  WHERE is_deleted = FALSE

), prep_crm_opportunity AS (

  SELECT *
  FROM {{ ref('prep_crm_opportunity') }}
  WHERE is_deleted = FALSE

), prep_crm_account AS (

  SELECT *
  FROM {{ ref('prep_crm_account') }}
  WHERE is_deleted = FALSE

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
    CASE
      WHEN prep_crm_opportunity.dim_crm_opportunity_id IS NOT NULL
        THEN prep_crm_task.dim_crm_opportunity_id
      WHEN account_opp_mapping.dim_crm_opportunity_id IS NOT NULL
        THEN account_opp_mapping.dim_crm_opportunity_id
      ELSE {{ get_keyed_nulls('prep_crm_task.dim_crm_opportunity_id') }}
      END as dim_mapped_opportunity_id,

    prep_crm_task.sfdc_record_id,

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

    -- Logic
    CASE 
      WHEN prep_crm_opportunity.dim_crm_opportunity_id IS NOT NULL 
        THEN 'Opportunity'
      WHEN account_opp_mapping.dim_crm_opportunity_id IS NOT NULL 
        THEN 'Account'
      ELSE 'Not Mappable'
      END AS task_mapped_to,

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
  LEFT JOIN prep_crm_person
    ON prep_crm_task.sfdc_record_id = prep_crm_person.sfdc_record_id
  LEFT JOIN prep_crm_opportunity
    ON prep_crm_task.dim_crm_opportunity_id = prep_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN dim_date
    ON {{ get_date_id('prep_crm_task.task_date') }} = dim_date.dim_date_id
  LEFT JOIN prep_crm_opportunity AS account_opp_mapping 
    ON prep_crm_task.account_or_opportunity_id = dim_crm_opportunity.dim_crm_account_id
    AND prep_crm_task.task_date < account_opp_mapping.close_date
    AND prep_crm_task.task_date >= DATEADD('month', -9, dim_date.first_day_of_fiscal_quarter)
    AND prep_crm_task.sa_activity_type IS NOT NULL

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@jngCES",
    created_date="2022-12-05",
    updated_date="2023-08-23"
) }}
