WITH source AS (

    SELECT *
    FROM {{ ref('ao_advanced_outreach_company') }}

)

SELECT *
FROM source