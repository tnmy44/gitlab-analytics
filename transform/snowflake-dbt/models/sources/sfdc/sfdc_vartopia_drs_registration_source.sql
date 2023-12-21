WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'vartopia_drs_registration') }}

), renamed AS (
    SELECT
      ID::VARCHAR AS registration_id,
      VartopiaDRS__Vendor_Deal_ID__c::VARCHAR      AS deal_id,
      Name::VARCHAR                                AS registration_name,
      CAM_Approval_Status__c::VARCHAR.             AS partner_manager_approval_status,
      VartopiaDRS__DR_status1__c::VARCHAR          AS deal_registration_status,
      VartopiaDRS__Approved_Date__c::VARCHAR       AS deal_registration_approval_date,
      VartopiaDRS__Picklist_1__c::VARCHAR          AS deal_registration_type,
      VartopiaDRS__MDF_Campaign__c::VARCHAR        AS gitLab_marketing_campaign,
      VartopiaDRS__Campaign_1__c::VARCHAR          AS distributor_marketing_campaign,

      -- metadata
      createdbyid::VARCHAR                         AS created_by_id,
      createddate::TIMESTAMP                       AS created_date,
      isdeleted::BOOLEAN                           AS is_deleted,
      lastmodifiedbyid::VARCHAR                    AS last_modified_by_id,
      lastmodifieddate::TIMESTAMP                  AS last_modified_date,
      _sdc_received_at::TIMESTAMP                  AS sfdc_received_at,
      _sdc_extracted_at::TIMESTAMP                 AS sfdc_extracted_at,
      _sdc_table_version::NUMBER                   AS sfdc_table_version,
      _sdc_batched_at::TIMESTAMP                   AS sfdc_batched_at,
      _sdc_sequence::NUMBER                        AS sfdc_sequence,
      systemmodstamp::TIMESTAMP                    AS system_mod_stamp

    FROM source
)
SELECT *
FROM renamed
