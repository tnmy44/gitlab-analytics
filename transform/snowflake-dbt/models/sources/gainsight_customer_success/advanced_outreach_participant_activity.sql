WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','advanced_outreach_participant_activity') }}
),

renamed AS (

  SELECT

  FROM source
)

SELECT *
FROM renamed