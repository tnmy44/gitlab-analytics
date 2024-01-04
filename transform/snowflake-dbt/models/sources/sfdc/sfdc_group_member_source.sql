WITH source AS (
    
    SELECT * 
    FROM {{ source('salesforce', 'group_member') }}

), renamed AS (

       SELECT
      
         --keys
         id AS group_member_id,
         groupid AS group_id,
         userorgroupid AS user_or_group_id,

         --metadata
         systemmodstamp AS system_mod_stamp,
         _sdc_batched_at AS sfdc_batched_at,
         _sdc_extracted_at AS sfdc_extracted_at,
         _sdc_received_at AS sfdc_received_at,
         _sdc_sequence AS sfdc_sequence,
         _sdc_table_version AS sfdc_table_version
      
       FROM source
)


SELECT *
FROM renamed

