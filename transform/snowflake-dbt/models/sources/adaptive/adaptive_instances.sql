SELECT
parse_json(_data)['@code']::varchar AS code,
parse_json(_data)['@name']::varchar AS name,
parse_json(_data)['@sessionTimeout']::varchar AS session_timeout,
parse_json(_data)['@systemCurrency']::varchar AS system_currency,
parse_json(_data)['@products']::varchar AS products,
parse_json(_data)['@options']::varchar AS options,
parse_json(_data)['@datasources']::varchar AS data_sources,
parse_json(_data)['@tenantCode']::varchar AS tenant_code,
parse_json(_data)['@tenantEnv']::varchar as tenant_env,
__LOADED_AT
FROM
  RAW.TAP_ADAPTIVE.instances
