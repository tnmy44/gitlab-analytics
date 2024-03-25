WITH source AS (

  SELECT *

  FROM {{ ref('gitlab_dotcom_catalog_resource_versions_dedupe_source') }}

),

renamed AS (

  SELECT



    id,
    release_id,
    catalog_resource_id,
    project_id,
    created_at,
    released_at,
    semver_major,
    semver_minor,
    semver_patch,
    semver_prerelease
  FROM source

)


SELECT *
FROM renamed
