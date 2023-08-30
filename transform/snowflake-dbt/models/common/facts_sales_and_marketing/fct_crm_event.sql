{{ simple_cte([
    ('prep_crm_person', 'prep_crm_person')
]) }}

, prep_crm_event AS (

  SELECT *
  FROM {{ ref('prep_crm_event') }} 
  WHERE is_deleted = FALSE

), final AS (

  SELECT
    -- Primary key
    prep_crm_event.dim_crm_event_pk,

    -- Foreign keys
    {{ get_keyed_nulls('prep_crm_event.dim_crm_event_sk') }}       AS dim_crm_event_sk,
    {{ get_keyed_nulls('prep_crm_event.dim_crm_account_id') }}     AS dim_crm_account_id,
    {{ get_keyed_nulls('prep_crm_event.dim_crm_user_id') }}        AS dim_crm_user_id,
    {{ get_keyed_nulls('prep_crm_person.dim_crm_person_id') }}     AS dim_crm_person_id,
    {{ get_keyed_nulls('prep_crm_event.dim_crm_opportunity_id') }} AS dim_crm_opportunity_id,
    prep_crm_event.sfdc_record_id,

    -- Dates
    {{ get_date_id('prep_crm_event.event_date') }}                 AS event_date_id,
    prep_crm_event.event_date,
    {{ get_date_id('prep_crm_event.event_end_date') }}             AS event_end_date_id,
    prep_crm_event.event_end_date,
    {{ get_date_id('prep_crm_event.reminder_date_time') }}         AS reminder_date_id,
    prep_crm_event.reminder_date_time,
    {{ get_date_id('prep_crm_event.event_recurrence_end_date') }}  AS event_recurrence_end_date_id,
    prep_crm_event.event_recurrence_end_date,
    {{ get_date_id('prep_crm_event.event_recurrence_start_date_time') }} AS event_recurrence_start_date_id,
    prep_crm_event.event_recurrence_start_date_time,


    -- Metadata
    prep_crm_event.created_by_id                                   AS event_created_by_id,
    {{ get_date_id('prep_crm_event.created_at') }}                 AS event_created_date_id,
    prep_crm_event.created_at                                      AS event_created_date,
    prep_crm_event.last_modified_id,
    {{ get_date_id('prep_crm_event.last_modified_date') }}         AS last_modified_date_id,
    prep_crm_event.last_modified_date
  FROM prep_crm_event
  LEFT JOIN prep_crm_person
    ON prep_crm_event.sfdc_record_id = prep_crm_person.sfdc_record_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-08-22",
    updated_date="2023-08-23"
) }}