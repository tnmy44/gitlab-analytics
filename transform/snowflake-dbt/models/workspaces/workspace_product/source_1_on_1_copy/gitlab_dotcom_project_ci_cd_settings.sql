WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_project_ci_cd_settings_source') }}

)

SELECT *
FROM source
