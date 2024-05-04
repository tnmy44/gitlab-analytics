{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

/* grain: lowest grain a refund made off the invoice */

{{ simple_cte([
    ('zuora_refund_invoice_payment_source', 'zuora_refund_invoice_payment_source'),
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
    {{ dbt_utils.generate_surrogate_key(['zuora_refund_invoice_payment_source.refund_invoice_payment_id']) }} AS refund_invoice_payment_pk,

    --Natural key 
    zuora_refund_invoice_payment_source.refund_invoice_payment_id,

    --Foreign keys
    zuora_refund_invoice_payment_source.invoice_id                                                         AS dim_invoice_id,
    zuora_refund_invoice_payment_source.refund_id,
    zuora_refund_invoice_payment_source.payment_id,
    zuora_account.dim_billing_account_id,
    zuora_refund_invoice_payment_source.accounting_period_id,


    --Refund invoice payment dates
    zuora_refund_invoice_payment_source.refund_invoice_payment_date,
    {{ get_date_id('zuora_refund_invoice_payment_source.refund_invoice_payment_date') }} AS refund_invoice_payment_date_id,

    --Additive fields
    zuora_refund_invoice_payment_source.refund_invoice_payment_amount



  FROM zuora_refund_invoice_payment_source
  INNER JOIN zuora_account
    ON zuora_refund_invoice_payment_source.account_id = zuora_account.dim_billing_account_id
)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-01",
updated_date="2024-05-01"
) }}
