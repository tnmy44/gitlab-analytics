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
  account.value['@accountTypeCode']::varchar      AS account_type_code,
  account.value['@decimalPrecision']::varchar     AS decimal_precision,
  account.value['@isAssumption']::varchar         AS is_assumption,
  account.value['@suppressZeroes']::varchar       AS suppress_zeroes,
  account.value['@isDefaultRoot']::varchar        AS is_default_root,
  account.value['@shortName']::varchar            AS short_name,
  account.value['@balanceType']::varchar          AS balance_type,
  account.value['@isLinked']::varchar             AS is_linked,
  account.value['@owningSheetId']::varchar        AS owning_sheet_id,
  account.value['@isSystem']::varchar             AS is_system,
  account.value['@isIntercompany']::varchar       AS is_intercompany,
  account.value['@dataEntryType']::varchar        AS data_entry_type,
  account.value['@planBy']::varchar               AS plan_by,
  account.value['@actualsBy']::varchar            AS actuals_by,
  account.value['@timeRollup']::varchar           AS time_rollup,
  account.value['@timeWeightAcctId']::varchar     AS time_weight_acct_id,
  account.value['@levelDimRollup']::varchar       AS level_dim_rollup,
  account.value['@levelDimWeightAcctId']::varchar AS level_dim_weight_acct_id,
  account.value['@rollupText']::varchar           AS rollup_text,
  account.value['@startExpanded']::varchar        AS start_expanded,
  account.value['@hasSalaryDetail']::varchar      AS has_salary_detail,
  account.value['@dataPrivacy']::varchar          AS data_privacy,
  account.value['@isBreakbackEligible']::varchar  AS is_breakback_eligible,
  account.value['@subType']::varchar              AS sub_type,
  account.value['@enableActuals']::varchar        AS enable_actuals,
  account.value['@isGroup']::varchar              AS is_group,
  account.value['@hasFormula']::varchar           AS has_formula,
  __loaded_at
FROM
  source,
  LATERAL FLATTEN(input => PARSE_JSON(_data) ['account']) account
ORDER BY
  __loaded_at
