WITH
source AS (
  SELECT * FROM
    {{ source('sirt_alertapp', 'sirt_alertapp_data_test') }}
),

dedupped AS (
  SELECT *
  FROM source
  QUALIFY ROW_NUMBER() OVER (PARTITION BY alertid ORDER BY gcs_modified_at DESC) = 1
)

SELECT *
FROM dedupped
