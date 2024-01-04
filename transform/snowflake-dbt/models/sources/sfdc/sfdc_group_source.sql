WITH source AS (
    
    SELECT * 
    FROM {{ source('salesforce', 'group') }}

), renamed AS (

       SELECT
      
         --keys
         id AS group_id,

         --info
         name AS group_name,
         relatedid AS related_id,
         type AS group_type,
         doesincludebosses AS is_does_include_bosses,

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

