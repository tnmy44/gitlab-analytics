WITH base AS (

    SELECT *
    FROM {{ ref('prep_major_minor_release') }}

)

SELECT *
FROM base
