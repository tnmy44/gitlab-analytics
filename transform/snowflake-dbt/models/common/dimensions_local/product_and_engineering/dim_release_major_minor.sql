WITH base AS (

    SELECT *
    FROM {{ ref('prep_release_major_minor') }}

)

SELECT *
FROM base
