{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

/* grain: lowest grain is a invoice item adjustment made on the invoice */

{{ simple_cte([
    ('zuora_invoice_item_adjustment_source', 'zuora_invoice_item_adjustment_source'),
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
    zuora_invoice_item_adjustment_source.invoice_item_adjustment_id,

    -- keys
    zuora_invoice_item_adjustment_source.invoice_item_adjustment_number,
    zuora_invoice_item_adjustment_source.account_id,
    zuora_invoice_item_adjustment_source.invoice_id,
    zuora_invoice_item_adjustment_source.accounting_period_id,


    -- invoice item adjustment dates
    zuora_invoice_item_adjustment_source.invoice_item_adjustment_date,
    {{ get_date_id('zuora_invoice_item_adjustment_source.invoice_item_adjustment_date') }} AS invoice_item_adjustment_date_id,

    -- additive fields
    zuora_invoice_item_adjustment_source.invoice_item_adjustment_amount,
    zuora_invoice_item_adjustment_source.invoice_item_adjustment_status,
    zuora_invoice_item_adjustment_source.invoice_item_adjustment_type


  FROM zuora_invoice_item_adjustment_source
  INNER JOIN zuora_account
    ON zuora_invoice_item_adjustment_source.account_id = zuora_account.account_id
)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-04-30",
updated_date="2024-04-30"
) }}
