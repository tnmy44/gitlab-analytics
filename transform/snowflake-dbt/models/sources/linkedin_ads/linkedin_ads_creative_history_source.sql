
WITH source AS (
  SELECT *
  FROM {{ source('linkedin_ads','creative_history') }}
),

renamed AS (

  SELECT
	id::NUMBER AS creative_id,
	last_modified_time::TIMESTAMP as last_modified_time,
	type::VARCHAR as type,
	created_time::TIMESTAMP as created_time,
	status::VARCHAR as status,
	click_uri::VARCHAR as click_uri,
	text_ad_text::VARCHAR as text_ad_text,
	text_ad_title::VARCHAR as text_ad_title,
	campaign_id::NUMBER as campaign_id,
	_fivetran_synced::TIMESTAMP AS _fivetran_synced

  FROM source
)

SELECT *
FROM renamed