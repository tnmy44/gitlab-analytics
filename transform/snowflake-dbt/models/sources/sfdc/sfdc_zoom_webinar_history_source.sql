
{{ config(
    tags=["mnpi_exception"]
) }}
WITH base AS (

    SELECT *
    FROM {{ source('salesforce', 'zoom_webinar_history') }}

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
    zoom_app__contact__c::text AS zoom_app_contact,
    zoom_app__is_attendeed__c::boolean AS zoom_app_is_attendeed,
    zoom_app__lead__c::text AS zoom_app_lead,
    zoom_app__webinar_topic__c::text AS zoom_app_webinar_topic,
    zoom_app__zoom_webinar_attendee__c::text AS zoom_app_zoom_webinar_attendee,
    zoom_app__zoom_webinar_panelist__c::text AS zoom_app_zoom_webinar_panelist,
    zoom_app__zoom_webinar_registrant__c::text AS zoom_app_zoom_webinar_registrant,
    zoom_app__zoom_webinar__c::text AS zoom_app_zoom_webinar,
    _sdc_batched_at::timestamp_tz AS _sdc_batched_at,
    _sdc_extracted_at::timestamp_tz AS _sdc_extracted_at,
    _sdc_received_at::timestamp_tz AS _sdc_received_at,
    _sdc_sequence::number AS _sdc_sequence,
    _sdc_table_version::number AS _sdc_table_version,
    lastreferenceddate::timestamp_tz AS last_referenced_date,
    lastvieweddate::timestamp_tz AS last_viewed_date
    from base

  )

SELECT *
FROM renamed
