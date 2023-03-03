
WITH source AS (
  SELECT *
  FROM {{ source('linkedin_ads','ad_analytics_by_creative') }}
),

renamed AS (

  SELECT
    creative_id::NUMBER as creative_id,
	day::TIMESTAMP as day,
	clicks::NUMBER as clicks,
	impressions::NUMBER as impressions,
	one_click_leads::NUMBER as one_click_leads,
	opens::NUMBER as opens,
	cost_in_usd::NUMBER as cost_in_usd
	_fivetran_synced timestamp_tz(9),

  FROM source
)

SELECT *
FROM renamed