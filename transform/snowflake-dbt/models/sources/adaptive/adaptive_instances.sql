WITH source AS (
  SELECT * FROM
    {{ source('adaptive', 'instances') }}
)

SELECT
  PARSE_JSON(_data) ['@code']::VARCHAR           AS code,
  PARSE_JSON(_data) ['@name']::VARCHAR           AS name,
  PARSE_JSON(_data) ['@sessionTimeout']::VARCHAR AS session_timeout,
  PARSE_JSON(_data) ['@systemCurrency']::VARCHAR AS system_currency,
  PARSE_JSON(_data) ['@products']::VARCHAR       AS products,
  PARSE_JSON(_data) ['@options']::VARCHAR        AS options,
  PARSE_JSON(_data) ['@datasources']::VARCHAR    AS data_sources,
  PARSE_JSON(_data) ['@tenantCode']::VARCHAR     AS tenant_code,
  PARSE_JSON(_data) ['@tenantEnv']::VARCHAR      AS tenant_env,
  __loaded_at
FROM
  source
