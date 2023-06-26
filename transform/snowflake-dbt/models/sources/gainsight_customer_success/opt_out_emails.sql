WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','opt_out_emails') }}
),

renamed AS (

  SELECT

  FROM source
)

SELECT *
FROM renamed