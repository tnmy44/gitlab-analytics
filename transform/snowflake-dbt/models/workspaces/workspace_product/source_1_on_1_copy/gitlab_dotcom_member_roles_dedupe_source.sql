WITH source AS (

    SELECT 
    *
    FROM {{ ref('gitlab_dotcom_member_roles_dedupe_source') }}

)

SELECT *
FROM source
