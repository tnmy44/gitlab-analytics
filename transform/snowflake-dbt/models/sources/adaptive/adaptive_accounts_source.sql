WITH source AS (
  SELECT * FROM {{ source('adaptive', 'accounts') }}
)

SELECT
  PARSE_JSON(source._data) ['@id']::VARCHAR              AS parent_id,
  PARSE_JSON(source._data) ['@name']::VARCHAR            AS parent_name,
  PARSE_JSON(source._data) ['@timeStratum']::VARCHAR     AS parent_time_stratum,
  PARSE_JSON(source._data) ['@accountTypeCode']::VARCHAR AS parent_account_type_code,
  fin_account.value['@id']::VARCHAR                      AS id,
  fin_account.value['@code']::VARCHAR                    AS code,
  fin_account.value['@name']::VARCHAR                    AS account_name,
  fin_account.value['@description']::VARCHAR             AS description,
  fin_account.value['@timeStratum']::VARCHAR             AS timestratum,
  fin_account.value['@displayAs']::VARCHAR               AS displayas,
  fin_account.value['@accountTypeCode']::VARCHAR         AS account_type_code,
  fin_account.value['@decimalPrecision']::VARCHAR        AS decimal_precision,
  fin_account.value['@isAssumption']::VARCHAR            AS is_assumption,
  fin_account.value['@suppressZeroes']::VARCHAR          AS suppress_zeroes,
  fin_account.value['@isDefaultRoot']::VARCHAR           AS is_default_root,
  fin_account.value['@shortName']::VARCHAR               AS short_name,
  fin_account.value['@balanceType']::VARCHAR             AS balance_type,
  fin_account.value['@isLinked']::VARCHAR                AS is_linked,
  fin_account.value['@owningSheetId']::VARCHAR           AS owning_sheet_id,
  fin_account.value['@isSystem']::VARCHAR                AS is_system,
  fin_account.value['@isIntercompany']::VARCHAR          AS is_intercompany,
  fin_account.value['@dataEntryType']::VARCHAR           AS data_entry_type,
  fin_account.value['@planBy']::VARCHAR                  AS plan_by,
  fin_account.value['@actualsBy']::VARCHAR               AS actuals_by,
  fin_account.value['@timeRollup']::VARCHAR              AS time_rollup,
  fin_account.value['@timeWeightAcctId']::VARCHAR        AS time_weight_acct_id,
  fin_account.value['@levelDimRollup']::VARCHAR          AS level_dim_rollup,
  fin_account.value['@levelDimWeightAcctId']::VARCHAR    AS level_dim_weight_acct_id,
  fin_account.value['@rollupText']::VARCHAR              AS rollup_text,
  fin_account.value['@startExpanded']::VARCHAR           AS start_expanded,
  fin_account.value['@hasSalaryDetail']::VARCHAR         AS has_salary_detail,
  fin_account.value['@dataPrivacy']::VARCHAR             AS data_privacy,
  fin_account.value['@isBreakbackEligible']::VARCHAR     AS is_breakback_eligible,
  fin_account.value['@subType']::VARCHAR                 AS sub_type,
  fin_account.value['@enableActuals']::VARCHAR           AS enable_actuals,
  fin_account.value['@isGroup']::VARCHAR                 AS is_group,
  fin_account.value['@hasFormula']::VARCHAR              AS has_formula,
  source.__loaded_at                                     AS uploaded_at
FROM
  source,
  LATERAL FLATTEN(input => PARSE_JSON(source._data) ['account']) AS fin_account
ORDER BY
  source.__loaded_at
