WITH source AS (

    SELECT *
    FROM {{ source('nvd_extract', 'cve_details_nvd') }}

)
SELECT *
FROM source