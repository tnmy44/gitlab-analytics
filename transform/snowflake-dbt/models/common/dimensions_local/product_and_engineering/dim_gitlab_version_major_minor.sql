WITH base AS (

    SELECT *
    FROM {{ ref('prep_gitlab_version_major_minor') }}

)

SELECT *
FROM base
