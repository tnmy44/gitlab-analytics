{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

/* grain: lowest grain is a refund made out off an invoice or account */

{{ simple_cte([
    ('zuora_refund_source', 'zuora_refund_source'),
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
    {{ dbt_utils.generate_surrogate_key(['zuora_refund_source.refund_id']) }} AS refund_pk,

    --Natural key 
    zuora_refund_source.refund_id,

    --Foreign keys
    zuora_account.dim_billing_account_id,
    zuora_refund_source.refund_number,


    --Refund dates
    zuora_refund_source.refund_date,
    {{ get_date_id('zuora_refund_source.refund_date') }} AS refund_date_id,

    --Degenerative dimensions
    zuora_refund_source.refund_status,
    zuora_refund_source.refund_type,

    --Additive fields
    zuora_refund_source.refund_amount



  FROM zuora_refund_source
  INNER JOIN zuora_account
    ON zuora_refund_source.account_id = zuora_account.dim_billing_account_id
)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-01",
updated_date="2024-05-01"
) }}
