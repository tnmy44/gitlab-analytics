WITH source AS (

   SELECT 
        parse_json(payload) as payload,
        uploaded_at
   FROM {{ source('e2e_metrics','fabrication_method') }}
 
), final AS (

    SELECT
        payload:fields.fabrication_time::NUMBER as fabrication_time,
        payload:fields.info::VARCHAR as info,
        payload:fields.job_url::VARCHAR as job_url,
        payload:fields.timestamp::VARCHAR as timestamp,
        payload:name::VARCHAR as name,
        payload:time::VARCHAR as time,
        payload:tags.fabrication_method::VARCHAR as fabrication_method,
        payload:tags.http_method::VARCHAR as http_method,
        payload:tags.merge_request::VARCHAR as merge_request,
        payload:tags.resource::VARCHAR as resource,
        payload:tags.run_type::VARCHAR as run_type,
        uploaded_at as uploaded_at
       FROM source
)

SELECT *
FROM final
