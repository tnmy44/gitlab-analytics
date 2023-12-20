
{{ config(
    tags=["mnpi_exception"]
) }}

WITH base AS (

    SELECT *
    FROM {{ source('salesforce', 'zoom_webinar_registrant') }}
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
    zoom_app__email__c::text AS zoom_app_email,
    zoom_app__external_key__c::text AS zoom_app_external_key,
    zoom_app__first_name__c::text AS zoom_app_first_name,
    zoom_app__job_title__c::text AS zoom_app_job_title,
    zoom_app__join_url1__c::text AS zoom_app_join_url1,
    zoom_app__last_name__c::text AS zoom_app_last_name,
    zoom_app__org__c::text AS zoom_app_org,
    zoom_app__status__c::text AS zoom_app_status,
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
