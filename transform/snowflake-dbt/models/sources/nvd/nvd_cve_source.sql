WITH source AS (

    SELECT *
    FROM {{ source('nvd', 'nvd_cve') }}

)
SELECT *
FROM source