WITH source AS (

  SELECT *

  FROM {{ ref('gitlab_dotcom_catalog_resources_dedupe_source') }}

),

renamed AS (

  SELECT

    id,
    project_id,
    created_at,
    state,
    latest_released_at,
    name,
    description,
    visibility_level,
    search_vector,
    verification_level,
    last_30_day_usage_count,
    last_30_day_usage_count_updated_at
  FROM source

)


SELECT *
FROM renamed
