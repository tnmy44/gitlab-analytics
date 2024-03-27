{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH final AS (

/* Determine the amount of payments applied to invoices monthly */

SELECT 
DATE_TRUNC('month',(DATE(invoice_start_date))) AS billing_period,
SUM(invoice_amount) AS pending_invoice_amount,
COUNT(invoice_amount) AS pending_invoice_count
FROM {{ ref('wk_finance_fct_invoice_payment') }} 
GROUP BY billing_period
ORDER BY billing_period

)


{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-27",
updated_date="2024-03-27"
) }}

