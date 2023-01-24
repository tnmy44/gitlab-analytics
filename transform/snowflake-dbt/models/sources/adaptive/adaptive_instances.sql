WITH source AS (
  SELECT * FROM
    {{ source('adaptive', 'instances') }}
),

SELECT
  PARSE_JSON(_data) ['@code']::varchar           AS code,
  PARSE_JSON(_data) ['@name']::varchar           AS name,
  PARSE_JSON(_data) ['@sessionTimeout']::varchar AS session_timeout,
  PARSE_JSON(_data) ['@systemCurrency']::varchar AS system_currency,
  PARSE_JSON(_data) ['@products']::varchar       AS products,
  PARSE_JSON(_data) ['@options']::varchar        AS options,
  PARSE_JSON(_data) ['@datasources']::varchar    AS data_sources,
  PARSE_JSON(_data) ['@tenantCode']::varchar     AS tenant_code,
  PARSE_JSON(_data) ['@tenantEnv']::varchar      AS tenant_env,
  __loaded_at
FROM
  source
