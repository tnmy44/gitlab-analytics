WITH source AS (

  SELECT *
  FROM {{ ref('nvd_cve_source') }}

)

SELECT *
FROM source