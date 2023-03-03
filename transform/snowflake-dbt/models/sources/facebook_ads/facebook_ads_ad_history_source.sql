WITH source AS (
  SELECT *
  FROM {{ source('facebook_ads','ad_history') }}
),

renamed AS (

  SELECT
	id::NUMBER as ad_id,
	updated_time::TIMESTAMP as updated_time,
	account_id::NUMBER as account_id,
	campaign_id::NUMBER as campaign_id,
	creative_id::NUMBER as creative_id,
	name::VARCHAR as name,
	status::VARCHAR as status,
	ad_set_id::NUMBER as ad_set_id,
    _fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed