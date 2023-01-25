WITH source AS (
  SELECT * FROM
    {{ source('adaptive', 'accounts') }}
)

SELECT
  PARSE_JSON(_data) ['@id']::VARCHAR              AS parent_id,
  PARSE_JSON(_data) ['@name']::VARCHAR            AS parent_name,
  PARSE_JSON(_data) ['@timeStratum']::VARCHAR     AS parent_time_stratum,
  PARSE_JSON(_data) ['@accountTypeCode']::VARCHAR AS parent_account_type_code,
  account.value['@id']::VARCHAR                   AS id,
  account.value['@code']::VARCHAR                 AS code,
  account.value['@name']::VARCHAR                 AS name,
  account.value['@description']::VARCHAR          AS description,
  account.value['@timeStratum']::VARCHAR          AS timestratum,
  account.value['@displayAs']::VARCHAR            AS displayas,
  account.value['@accountTypeCode']::VARCHAR      AS account_type_code,
  account.value['@decimalPrecision']::VARCHAR     AS decimal_precision,
  account.value['@isAssumption']::VARCHAR         AS is_assumption,
  account.value['@suppressZeroes']::VARCHAR       AS suppress_zeroes,
  account.value['@isDefaultRoot']::VARCHAR        AS is_default_root,
  account.value['@shortName']::VARCHAR            AS short_name,
  account.value['@balanceType']::VARCHAR          AS balance_type,
  account.value['@isLinked']::VARCHAR             AS is_linked,
  account.value['@owningSheetId']::VARCHAR        AS owning_sheet_id,
  account.value['@isSystem']::VARCHAR             AS is_system,
  account.value['@isIntercompany']::VARCHAR       AS is_intercompany,
  account.value['@dataEntryType']::VARCHAR        AS data_entry_type,
  account.value['@planBy']::VARCHAR               AS plan_by,
  account.value['@actualsBy']::VARCHAR            AS actuals_by,
  account.value['@timeRollup']::VARCHAR           AS time_rollup,
  account.value['@timeWeightAcctId']::VARCHAR     AS time_weight_acct_id,
  account.value['@levelDimRollup']::VARCHAR       AS level_dim_rollup,
  account.value['@levelDimWeightAcctId']::VARCHAR AS level_dim_weight_acct_id,
  account.value['@rollupText']::VARCHAR           AS rollup_text,
  account.value['@startExpanded']::VARCHAR        AS start_expanded,
  account.value['@hasSalaryDetail']::VARCHAR      AS has_salary_detail,
  account.value['@dataPrivacy']::VARCHAR          AS data_privacy,
  account.value['@isBreakbackEligible']::VARCHAR  AS is_breakback_eligible,
  account.value['@subType']::VARCHAR              AS sub_type,
  account.value['@enableActuals']::VARCHAR        AS enable_actuals,
  account.value['@isGroup']::VARCHAR              AS is_group,
  account.value['@hasFormula']::VARCHAR           AS has_formula,
  __loaded_at
FROM
  source,
  LATERAL FLATTEN(input => PARSE_JSON(_data) ['account']) account
ORDER BY
  __loaded_at
