
WITH source AS (
  SELECT *
  FROM {{ source('sheetload', 'devrel_influenced_campaings') }}
), renamed AS (
  SELECT 
    campaign_name::VARCHAR  as campaign_name,
    description::VARCHAR    as description,
    influence_type::VARCHAR as influence_type

  FROM source
)
SELECT *
FROM renamed
WHERE campaign_name IS NOT NULL