{{ config(
    materialized=["view"]
    tags=["mnpi"]
) }}



WITH zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }} 
    WHERE is_deleted = FALSE

), zuora_payment AS (

/* grain: lowest grain one payment, one payment may be split and applied to several invoices */

    SELECT *
    FROM {{ source('zuora', 'payment') }}
    
), final_payment AS (

SELECT
   -- primary key 
     id                          						          AS payment_id,

   -- keys
      paymentnumber               					          AS payment_number,     
      accountid                  					            AS account_id,

   -- payment dates
     effectivedate                     					      AS payment_date,
     {{ get_date_id('zuora_payment.effectivedate') }} AS payment_date_id,

   -- additive fields
      status                      					          AS payment_status,
      type                     						            AS payment_type,
      amount                        					        AS payment_amount



FROM zuora_payment
INNER JOIN zuora_account
  ON zuora_payment.account_id = zuora_account.account_id
)

{{ dbt_audit(
cte_ref="final_payment",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-02-26",
updated_date="2024-02-26"
) }}
