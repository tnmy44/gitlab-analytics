WITH source as (

  SELECT
    {{ dbt_utils.star(from=source('gitlab_snowplow', 'events'), except=['geo_zipcode', 'geo_latitude', 'geo_longitude', 'user_ipaddress']) }}
  FROM {{ source('gitlab_snowplow', 'events') }}
  WHERE TRY_TO_TIMESTAMP(derived_tstamp)::DATE >= '2024-04-01'
    AND TRY_TO_TIMESTAMP(derived_tstamp)::DATE <= '2024-04-07'

)

SELECT *
FROM source
