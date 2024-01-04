WITH source AS (
    
    SELECT * 
    FROM {{ source('salesforce', 'opportunity_share') }}

), renamed AS (

       SELECT

         --keys
          id	AS	opportunity_share_id,
          opportunityid AS opportunity_id,
          userorgroupid AS user_or_group_id,
      
         --info 
         opportunityaccesslevel AS opportunity_access_level,
         lastmodifiedbyid AS last_modified_by_id,
         lastmodifieddate AS last_modified_date,
         rowcause AS row_cause,
         isdeleted AS is_deleted,

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

