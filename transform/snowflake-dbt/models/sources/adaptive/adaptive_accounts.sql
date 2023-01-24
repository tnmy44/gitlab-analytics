WITH source AS (
  SELECT * FROM
    {{ source('adaptive', 'accounts') }}
),

SELECT
  PARSE_JSON(_data) ['@id']::varchar              AS parent_id,
  PARSE_JSON(_data) ['@name']::varchar            AS parent_name,
  PARSE_JSON(_data) ['@timeStratum']::varchar     AS parent_time_stratum,
  PARSE_JSON(_data) ['@accountTypeCode']::varchar AS parent_account_type_code,
  account.value['@id']::varchar                   AS id,
  account.value['@code']::varchar                 AS code,
  account.value['@name']::varchar                 AS name,
  account.value['@description']::varchar          AS description,
  account.value['@timeStratum']::varchar          AS timestratum,
  account.value['@displayAs']::varchar            AS displayas,
  account.value['@accountTypeCode']::varchar      AS accounttypecode,
  account.value['@decimalPrecision']::varchar     AS decimalprecision,
  account.value['@isAssumption']::varchar         AS isassumption,
  account.value['@suppressZeroes']::varchar       AS suppresszeroes,
  account.value['@isDefaultRoot']::varchar        AS isdefaultroot,
  account.value['@shortName']::varchar            AS shortname,
  account.value['@balanceType']::varchar          AS balancetype,
  account.value['@isLinked']::varchar             AS islinked,
  account.value['@owningSheetId']::varchar        AS owningsheetid,
  account.value['@isSystem']::varchar             AS issystem,
  account.value['@isIntercompany']::varchar       AS isintercompany,
  account.value['@dataEntryType']::varchar        AS dataentrytype,
  account.value['@planBy']::varchar               AS planby,
  account.value['@actualsBy']::varchar            AS actualsby,
  account.value['@timeRollup']::varchar           AS timerollup,
  account.value['@timeWeightAcctId']::varchar     AS timeweightacctid,
  account.value['@levelDimRollup']::varchar       AS leveldimrollup,
  account.value['@levelDimWeightAcctId']::varchar AS leveldimweightacctid,
  account.value['@rollupText']::varchar           AS rolluptext,
  account.value['@startExpanded']::varchar        AS startexpanded,
  account.value['@hasSalaryDetail']::varchar      AS hassalarydetail,
  account.value['@dataPrivacy']::varchar          AS dataprivacy,
  account.value['@isBreakbackEligible']::varchar  AS isbreakbackeligible,
  account.value['@subType']::varchar              AS subtype,
  account.value['@enableActuals']::varchar        AS enableactuals,
  account.value['@isGroup']::varchar              AS isgroup,
  account.value['@hasFormula']::varchar           AS hasformula,
  __loaded_at
FROM
  source,
  LATERAL FLATTEN(input => PARSE_JSON(_data) ['account']) account
ORDER BY
  __loaded_at
