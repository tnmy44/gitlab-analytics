WITH source AS (
  SELECT *
  FROM {{ source('facebook_ads','basic_all_levels') }}
),

renamed AS (

  SELECT
	ad_id::VARCHAR as ad_id,
	date::DATE as date,
	account_id::NUMBER as account_id,
	impressions::NUMBER as impressions,
	inline_link_clicks::NUMBER as inline_link_clicks,
	spend::FLOAT as spend,
	ad_name::VARCHAR as ad_name,
	adset_name::VARCHAR as adset_name,
	campaign_name::VARCHAR as campaign_name,
    _fivetran_id::VARCHAR as _fivetran_id,
    _fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed