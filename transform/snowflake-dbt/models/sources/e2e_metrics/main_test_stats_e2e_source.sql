WITH source AS (

   SELECT 
        parse_json(payload) as payload,
        uploaded_at
   FROM {{ source('e2e_metrics','main_test_stats') }}
 
), final AS (

    SELECT
        payload:fields.api_fabrication::NUMBER as api_fabrication,
        payload:fields.failure_exception::VARCHAR as failure_exception,
        payload:fields.id::VARCHAR as id,
        payload:fields.job_id::VARCHAR as job_id,
        payload:fields.job_url::VARCHAR as job_url,
        payload:fields.pipeline_id::VARCHAR as pipeline_id,
        payload:fields.pipeline_url::VARCHAR as pipeline_url,
        payload:fields.run_time::NUMBER as run_time,
        payload:fields.total_fabrication::NUMBER as total_fabrication,
        payload:fields.ui_fabrication::NUMBER as ui_fabrication,
        payload:name::VARCHAR as name,
        payload:time::VARCHAR as time,
        payload:tags.blocking::VARCHAR as blocking,
        payload:tags.file_path::VARCHAR as file_path,
        payload:tags.job_name::VARCHAR as job_name,
        payload:tags.merge_request::VARCHAR as merge_request,
        payload:tags.name::VARCHAR as tags_name, -- ask sanad the name for it
        payload:tags.product_group::VARCHAR as product_group,
        payload:tags.quarantined::VARCHAR as quarantined,
        payload:tags.run_type::VARCHAR as run_type,
        payload:tags.smoke::VARCHAR as smoke,
        payload:tags.stage::VARCHAR as stage,
        payload:tags.status::VARCHAR as status,
        payload:tags.testcase::VARCHAR as testcase,
        uploaded_at as uploaded_at
       FROM source
)

SELECT *
FROM final
