with source as (
  select * from
  {{ source('tap_adaptive', 'accounts'}}
),
SELECT
  PARSE_JSON(_data)['@id']::varchar AS parent_id,
  PARSE_JSON(_data)['@name']::varchar AS parent_name,
  PARSE_JSON(_data)['@timeStratum']::varchar AS parent_time_stratum,
  PARSE_JSON(_data)['@accountTypeCode']::varchar AS parent_account_type_code,
  account.value['@id']::varchar AS id,
  account.value['@code']::varchar AS code,
  account.value['@name']::varchar AS name,
  account.value['@description']::varchar AS description,
  account.value['@timeStratum']::varchar AS timeStratum,
  account.value['@displayAs']::varchar AS displayAs,
  account.value['@accountTypeCode']::varchar AS accountTypeCode,
  account.value['@decimalPrecision']::varchar AS decimalPrecision,
  account.value['@isAssumption']::varchar AS isAssumption,
  account.value['@suppressZeroes']::varchar AS suppressZeroes,
  account.value['@isDefaultRoot']::varchar AS isDefaultRoot,
  account.value['@shortName']::varchar AS shortName,
  account.value['@balanceType']::varchar AS balanceType,
  account.value['@isLinked']::varchar AS isLinked,
  account.value['@owningSheetId']::varchar AS owningSheetId,
  account.value['@isSystem']::varchar AS isSystem,
  account.value['@isIntercompany']::varchar AS isIntercompany,
  account.value['@dataEntryType']::varchar AS dataEntryType,
  account.value['@planBy']::varchar AS planBy,
  account.value['@actualsBy']::varchar AS actualsBy,
  account.value['@timeRollup']::varchar AS timeRollup,
  account.value['@timeWeightAcctId']::varchar AS timeWeightAcctId,
  account.value['@levelDimRollup']::varchar AS levelDimRollup,
  account.value['@levelDimWeightAcctId']::varchar AS levelDimWeightAcctId,
  account.value['@rollupText']::varchar AS rollupText,
  account.value['@startExpanded']::varchar AS startExpanded,
  account.value['@hasSalaryDetail']::varchar AS hasSalaryDetail,
  account.value['@dataPrivacy']::varchar AS dataPrivacy,
  account.value['@isBreakbackEligible']::varchar AS isBreakbackEligible,
  account.value['@subType']::varchar AS subType,
  account.value['@enableActuals']::varchar AS enableActuals,
  account.value['@isGroup']::varchar AS isGroup,
  account.value['@hasFormula']::varchar AS hasFormula,
  __LOADED_AT
FROM
  source,
  LATERAL FLATTEN(input => PARSE_JSON(_data)['account']) account
ORDER BY
  __LOADED_AT
