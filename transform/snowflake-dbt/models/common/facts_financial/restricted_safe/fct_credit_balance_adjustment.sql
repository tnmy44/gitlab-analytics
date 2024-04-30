{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

/* grain: lowest grain is a credit balance adjustment made on the invoice or account */

{{ simple_cte([
    ('zuora_credit_balance_adjustment_source', 'zuora_credit_balance_adjustment_source'),
    ('zuora_account_source', 'zuora_account_source')
]) }},

zuora_account AS (

  SELECT *
  FROM zuora_account_source
  WHERE is_deleted = FALSE

),

final AS (

  SELECT
    -- primary key 
    zuora_credit_balance_adjustment_source.credit_balance_adjustment_id,

    -- keys
    zuora_credit_balance_adjustment_source.credit_balance_adjustment_number,
    zuora_credit_balance_adjustment_source.account_id,
    zuora_credit_balance_adjustment_source.invoice_id,
    zuora_credit_balance_adjustment_source.accounting_period_id,


    -- credit balance adjustment dates
    zuora_credit_balance_adjustment_source.credit_balance_adjustment_date,
    {{ get_date_id('zuora_credit_balance_adjustment_source.credit_balance_adjustment_date') }} AS credit_balance_adjustment_date_id,

    -- additive fields
    zuora_credit_balance_adjustment_source.credit_balance_adjustment_amount,
    zuora_credit_balance_adjustment_source.credit_balance_adjustment_status,
    zuora_credit_balance_adjustment_source.credit_balance_adjustment_type


  FROM zuora_credit_balance_adjustment_source
  INNER JOIN zuora_account
    ON zuora_credit_balance_adjustment_source.account_id = zuora_account.account_id
)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-04-30",
updated_date="2024-04-30"
) }}
