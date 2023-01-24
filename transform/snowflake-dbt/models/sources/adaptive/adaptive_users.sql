SELECT
parse_json(_data)['@id']::varchar AS id,
parse_json(_data)['@login']::varchar AS login,
parse_json(_data)['@email']::varchar AS email,
parse_json(_data)['@name']::varchar AS name,
parse_json(_data)['@permissionSetId']::varchar AS permission_set_id,
parse_json(_data)['@guid']::varchar AS guid,
parse_json(_data)['@timeZone']::varchar AS time_zone,
parse_json(_data)['subscriptions']::variant AS subscriptions,
__LOADED_AT
FROM
  RAW.TAP_ADAPTIVE.users
