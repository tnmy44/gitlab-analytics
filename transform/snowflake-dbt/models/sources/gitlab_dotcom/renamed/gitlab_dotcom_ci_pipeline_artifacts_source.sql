WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_pipeline_artifacts_dedupe_source') }}

), renamed AS (

  SELECT
    id::number as id,
    project_id::number as project_id,
    pipeline_id::timestamp as pipeline_id,
    file_type::number as file_type,
    size::number as size,
    created_at::timestamp as created_at,
    updated_at::timestamp as updated_at,
    expire_at::timestamp as expire_at,
    file::varchar as file,
    file_store::number as file_store,
    file_format::number as file_format,
    locked::number as locked
  FROM source

)


SELECT *
FROM renamed
