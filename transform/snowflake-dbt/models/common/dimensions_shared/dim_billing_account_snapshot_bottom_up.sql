{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('map_merged_crm_account','map_merged_crm_account'),
    ('zuora_contact','zuora_contact_source'),
    ('customers_snapshot', 'customers_db_customers_snapshots')
]) }}

, snapshot_dates AS (

   SELECT *
   FROM {{ ref('dim_date') }}
   WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE

), zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_snapshots_source') }}
    WHERE is_deleted = FALSE
      AND LOWER(live_batch) != 'batch20'

), zuora_account_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      zuora_account.*
    FROM zuora_account
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= zuora_account.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('zuora_account.dbt_valid_to') }}

), joined AS (

    SELECT
      zuora_account_spined.snapshot_id,
      zuora_account_spined.account_id                              AS dim_billing_account_id,
      map_merged_crm_account.dim_crm_account_id,
      zuora_account_spined.account_number                          AS billing_account_number,
      zuora_account_spined.account_name                            AS billing_account_name,
      zuora_account_spined.status                                  AS account_status,
      zuora_account_spined.parent_id,
      zuora_account_spined.sfdc_account_code                       AS crm_account_code,
      zuora_account_spined.sfdc_entity                             AS crm_entity,
      zuora_account_spined.currency                                AS account_currency,
      zuora_contact.country                                        AS sold_to_country,
      zuora_account_spined.ssp_channel,
      zuora_account_spined.po_required,
      zuora_billing_account.auto_pay,
      zuora_billing_account.default_payment_method_type,
      zuora_account_spined.is_deleted,
      zuora_account_spined.batch,
      'Y'                                                          AS exists_in_zuora
    FROM zuora_account_spined
    LEFT JOIN zuora_contact
      ON COALESCE(zuora_account_spined.sold_to_contact_id, zuora_account_spined.bill_to_contact_id) = zuora_contact.contact_id
    LEFT JOIN map_merged_crm_account
      ON zuora_account_spined.crm_id = map_merged_crm_account.sfdc_account_id

), cdot_billing_account_snapshot AS (

    SELECT 
      billing_account_id,
      zuora_account_id,
      zuora_account_name,
      sfdc_account_id,
      billing_account_created_at,
      billing_account_updated_at,
      'Y' AS exists_in_cdot
    FROM customers_snapshot
    --Exclude Batch20(test records) from CDot by using Zuora test account IDs.
    WHERE zuora_account_id NOT IN 
      (SELECT DISTINCT 
        account_id 
       FROM customers_snapshot
       WHERE LOWER(batch) = 'batch20')

), cdot_billing_account_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      cdot_billing_account_snapshot.*
    FROM cdot_billing_account_snapshot
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= cdot_billing_account_snapshot.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('cdot_billing_account_snapshot.dbt_to') }}



), final AS (

    SELECT
       --surrogate key
      {{ dbt_utils.surrogate_key(['COALESCE(joined.snapshot_id, cdot_billing_account_spined.snapshot_id)', 'COALESCE(joined.dim_billing_account_id, cdot_billing_account_spined.zuora_account_id)'])}} AS billing_account_snapshot_id,

      COALESCE(joined.snapshot_id, cdot_billing_account_spined.snapshot_id)                                                                                                                              AS snapshot_id,
      {{ dbt_utils.surrogate_key(['COALESCE(joined.dim_billing_account_id, cdot_billing_account_spined.zuora_account_id)']) }}                                                                           AS dim_billing_account_sk,

      --natural key
      COALESCE(joined.dim_billing_account_id, cdot_billing_account_spined.zuora_account_id)                                                                                                              AS dim_billing_account_id,

      --foreign key
      COALESCE(joined.dim_crm_account_id, cdot_billing_account_spined.sfdc_account_id)                                                                                                                   AS dim_crm_account_id,

      --other relevant attributes
      joined.billing_account_number,
      COALESCE(joined.billing_account_name, cdot_billing_account_spined.zuora_account_name)                                                                                                              AS billing_account_name,
      joined.account_status,
      joined.parent_id,
      joined.crm_account_code,
      joined.crm_entity,
      joined.account_currency,
      joined.sold_to_country,
      joined.ssp_channel,
      joined.po_required,
      joined.auto_pay,
      joined.default_payment_method_type,
      joined.is_deleted,
      joined.batch,
      CASE 
            WHEN exists_in_zuora = 'Y' and exists_in_cdot = 'Y' THEN 'exists in CDot & Zuora'
            WHEN exists_in_zuora = 'Y' and exists_in_cdot IS NULL THEN 'exists only in Zuora'
            WHEN exists_in_zuora IS NULL and exists_in_cdot = 'Y' THEN 'exists only in CDot'
            ELSE NULL 
      END 
    FROM joined
    FULL JOIN cdot_billing_account_spined
      ON joined.dim_billing_account_id = cdot_billing_account_spined.zuora_account_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@snalamaru",
    created_date="2021-08-09",
    updated_date="2023-06-06"
) }}
