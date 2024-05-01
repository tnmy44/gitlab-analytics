{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

/* grain: lowest grain: a payment applied to the invoice */

{{ simple_cte([
    ('zuora_invoice_payment_source', 'zuora_invoice_payment_source'),
    ('prep_billing_account', 'prep_billing_account')
]) }},

zuora_account AS (

  SELECT *
  FROM prep_billing_account
  WHERE is_deleted = FALSE

),

final AS (

  SELECT
    --Primary key 
    zuora_invoice_payment_source.invoice_payment_id                                                 AS invoice_payment_pk,

    --Foreign keys
    zuora_invoice_payment_source.invoice_id                                                         AS dim_invoice_id,
    zuora_invoice_payment_source.payment_id,
    zuora_account.dim_billing_account_id,
    zuora_invoice_payment_source.accounting_period_id,


    --Invoice payment dates
    zuora_invoice_payment_source.invoice_payment_date,
    {{ get_date_id('zuora_invoice_payment_source.invoice_payment_date') }} AS invoice_payment_date_id,

    --Additive fields
    zuora_invoice_payment_source.invoice_payment_amount



  FROM zuora_invoice_payment_source
  INNER JOIN zuora_account
    ON zuora_invoice_payment_source.account_id = zuora_account.dim_billing_account_id
)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-01",
updated_date="2024-05-01"
) }}
