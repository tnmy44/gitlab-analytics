WITH source AS (
    
    SELECT * 
    FROM {{ source('salesforce', 'account_share') }}

), renamed AS (

       SELECT

         --keys
          id	AS	account_share_id,
          accountid AS account_id,
          userorgroupid AS user_or_group_id,
      
         --info 
         accountaccesslevel AS account_access_level,
         caseaccesslevel AS case_access_level,
         contactaccesslevel AS contact_access_level,
         opportunityaccesslevel AS opportunity_access_level,
         isdeleted AS is_deleted,
         lastmodifiedbyid AS last_modified_by_id,
         lastmodifieddate AS last_modified_date,
         rowcause AS row_cause,

         --metadata
         _sdc_batched_at AS sfdc_batched_at,
         _sdc_extracted_at AS sfdc_extracted_at,
         _sdc_received_at AS sfdc_received_at,
         _sdc_sequence AS sfdc_sequence,
         _sdc_table_version AS sfdc_table_version
      
       FROM source
)


SELECT *
FROM renamed

