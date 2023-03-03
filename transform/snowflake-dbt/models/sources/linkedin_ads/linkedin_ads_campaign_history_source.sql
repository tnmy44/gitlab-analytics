
WITH source AS (
  SELECT *
  FROM {{ source('linkedin_ads','campaign_history') }}
),

renamed AS (

  SELECT
	id::NUMBER AS campaign_id,
	last_modified_time::TIMESTAMP as last_modified_time,
	type::VARCHAR as type,
	created_time::TIMESTAMP as created_time,
	name::VARCHAR as name,
	status::VARCHAR as status,
	format varchar(256),
	run_schedule_start::TIMESTAMP as run_schedule_start,
	run_schedule_end::TIMESTAMP as run_schedule_end,
	campaign_group_id::NUMBER as campaign_group_id,
	account_id::NUMBER as account_id,
	_fivetran_synced::TIMESTAMP AS _fivetran_synced

  FROM source
)

SELECT *
FROM renamed