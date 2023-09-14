WITH source AS (

  SELECT *
  FROM {{ ref('cve_details_nvd') }}

)

SELECT *
FROM source