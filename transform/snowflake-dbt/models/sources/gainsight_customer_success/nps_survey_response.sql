WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','') }}
),

renamed AS (

  SELECT

  FROM source
)

SELECT *
FROM renamed
