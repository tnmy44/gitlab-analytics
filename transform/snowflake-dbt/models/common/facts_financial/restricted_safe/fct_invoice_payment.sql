{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

/* grain: lowest grain: a payment applied to the invoice */

{{ simple_cte([
    ('zuora_invoice_payment_source', 'zuora_invoice_payment_source'),
    ('zuora_account_source', 'zuora_account_source')
]) }},

zuora_account AS (

    SELECT *
    FROM zuora_account_source
    WHERE is_deleted = FALSE
    
), final AS (

SELECT
   -- primary key 
      zuora_invoice_payment_source.invoice_payment_id,

   -- keys
      zuora_invoice_payment_source.invoice_id,
      zuora_invoice_payment_source.payment_id,     
      zuora_invoice_payment_source.account_id,
      zuora_invoice_payment_source.accounting_period_id,


   -- invoice payment dates
      zuora_invoice_payment_source.invoice_payment_date,
     {{ get_date_id('zuora_invoice_payment_source.createddate') }} AS invoice_payment_date_id,

   -- additive fields
      zuora_invoice_payment_source.invoice_payment_amount



FROM zuora_invoice_payment_source
INNER JOIN zuora_account
  ON zuora_invoice_payment_source.accountid = zuora_account.account_id
)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-04-30",
updated_date="2024-04-30"
) }}

