WITH source AS (
  SELECT *
  FROM {{ source('facebook_ads','creative_history') }}
),

renamed AS (

  SELECT
	account_id::NUMBER as account_id,
	id::NUMBER as creative_id,
	body::VARCHAR as body,
	link_destination_display_url::VARCHAR as link_destination_display_url,
	link_url::VARCHAR as link_url,
	name::VARCHAR as name,
	object_type::VARCHAR as object_type,
	object_url::VARCHAR as object_url,
	status::VARCHAR as status,
	title::VARCHAR as title,
	page_link::VARCHAR as page_link,
    _fivetran_id::VARCHAR as _fivetran_id,
    _fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed