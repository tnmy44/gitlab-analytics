{{ config(
    tags=["mnpi_exception"]
) }}

WITH base AS (

    SELECT *
    FROM {{ source('salesforce', 'zoom_webinar_attendee') }}
), renamed AS (
  select
  createdbyid::text AS created_by_id,
  createddate::timestamp_tz AS created_date,
  id::text AS id,
  isdeleted::boolean AS is_deleted,
  lastmodifiedbyid::text AS last_modified_by_id,
  lastmodifieddate::timestamp_tz AS last_modified_date,
  name::text AS name,
  ownerid::text AS owner_id,
  systemmodstamp::timestamp_tz AS system_mod_stamp,
  zoom_app__duration_hh_mi_ss__c::text AS zoom_app_duration_hh_mi_ss,
  zoom_app__duration__c::float AS zoom_app_duration,
  zoom_app__join_time__c::timestamp_tz AS zoom_app_join_time,
  zoom_app__leave_time__c::timestamp_tz AS zoom_app_leave_time,
  zoom_app__name__c::text AS zoom_app_name,
  zoom_app__user_email__c::text AS zoom_app_user_email,
  zoom_app__user_id__c::text AS zoom_app_user_id,
  zoom_app__uuid__c::text AS zoom_app_uuid,
  zoom_app__zoom_webinar_panelist__c::text AS zoom_app_zoom_webinar_panelist,
  zoom_app__zoom_webinar_registrant__c::text AS zoom_app_zoom_webinar_registrant,
  zoom_app__zoom_webinar__c::text AS zoom_app_zoom_webinar,
  _sdc_batched_at::timestamp_tz AS sdc_batched_at,
  _sdc_extracted_at::timestamp_tz AS _sdc_extracted_at,
  _sdc_received_at::timestamp_tz AS _sdc_received_at,
  _sdc_sequence::number AS _sdc_sequence,
  _sdc_table_version::number AS _sdc_table_version

    from base
  )

SELECT *
FROM renamed
