
{{ config(
    tags=["mnpi"]
) }}

WITH base AS (

    SELECT *
    FROM {{ source('salesforce', 'zoom_webinar') }}

), renamed AS (

    SELECT
    createdbyid::text AS created_by_id,
    createddate::timestamp_tz AS created_date,
    id::text AS id,
    isdeleted::boolean AS is_deleted,
    lastmodifiedbyid::text AS last_modified_by_id,
    lastmodifieddate::timestamp_tz AS last_modified_date,
    name::text AS name,
    ownerid::text AS owner_id,
    systemmodstamp::timestamp_tz AS system_mod_stamp,
    zoom_app__actual_end_time__c::timestamp_tz AS zoom_app_actual_end_time,
    zoom_app__actual_start_time__c::timestamp_tz AS zoom_app_actual_start_time,
    zoom_app__add_registrants_as_campaign_members__c::boolean AS zoom_app_add_registrants_as_campaign_members,
    zoom_app__agenda__c::text AS zoom_app_agenda,
    zoom_app__create_campaign_for_each_webinar__c::boolean AS zoom_app_create_campaign_for_each_webinar,
    zoom_app__create_new_lead__c::boolean AS zoom_app_create_new_lead,
    zoom_app__creation_status__c::text AS zoom_app_creation_status,
    zoom_app__duration__c::float AS zoom_app_duration,
    zoom_app__end_time__c::timestamp_tz AS zoom_app_end_time,
    zoom_app__join_url__c::text AS zoom_app_join_url,
    zoom_app__number_of_participants__c::float AS zoom_app_number_of_participants,
    zoom_app__password__c::text AS zoom_app_password,
    zoom_app__register_url__c::text AS zoom_app_register_url,
    zoom_app__setting_allow_multiple_devices__c::boolean AS zoom_app_setting_allow_multiple_devices,
    zoom_app__setting_approval_type__c::text AS zoom_app_setting_approval_type,
    zoom_app__setting_close_registration__c::boolean AS zoom_app_setting_close_registration,
    zoom_app__setting_host_video_on__c::boolean AS zoom_app_setting_host_video_on,
    zoom_app__setting_panelists_video_on__c::boolean AS zoom_app_setting_panelists_video_on,
    zoom_app__setting_practice_session__c::boolean AS zoom_app_setting_practice_session,
    zoom_app__start_time__c::timestamp_tz AS zoom_app_start_time,
    zoom_app__start_url__c::text AS zoom_app_start_url,
    zoom_app__status__c::text AS zoom_app_status,
    zoom_app__topic__c::text AS zoom_app_topic,
    zoom_app__uuid_unique__c::text AS zoom_app_uuid_unique,
    zoom_app__uuid__c::text AS zoom_app_uuid,
    zoom_app__webinar_id__c::text AS zoom_app_webinar_id,
    _sdc_batched_at::timestamp_tz AS sdc_batched_at,
    _sdc_extracted_at::timestamp_tz AS sdc_extracted_at,
    _sdc_received_at::timestamp_tz AS sdc_received_at,
    _sdc_sequence::number AS sdc_sequence,
    _sdc_table_version::number AS sdc_table_version,
    lastvieweddate::timestamp_tz AS last_viewed_date,
    lastreferenceddate::timestamp_tz AS last_referenced_date

  )

SELECT *
FROM renamed
