{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

/* grain: lowest grain is a refund made out off an invoice or account */

{{ simple_cte([
    ('zuora_refund_source', 'zuora_refund_source'),
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
      zuora_refund_source.refund_id,

   -- keys
      zuora_refund_source.account_id,
      zuora_refund_source.refund_number,


   -- refund dates
      zuora_refund_source.refund_date,
     {{ get_date_id('zuora_refund_source.refund_date') }} AS refund_date_id,

   -- additive fields
      zuora_refund_source.refund_amount,
      zuora_refund_source.refund_status,
      zuora_refund_source.refund_type




FROM zuora_refund_source
INNER JOIN zuora_account
  ON zuora_refund.account_id = zuora_account.account_id
)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-04-30",
updated_date="2024-04-30"
) }}

