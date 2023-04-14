WITH base AS (

    SELECT *
    FROM {{ ref('prep_application_release') }}

)

SELECT *
FROM base
