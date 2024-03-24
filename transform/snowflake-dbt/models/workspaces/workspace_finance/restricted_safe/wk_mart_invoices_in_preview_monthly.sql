{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH pending_invoice_aggregate AS

(

SELECT 
DATE_TRUNC('month',(DATE("Invoicestartdate"))) AS billing_period,
SUM("InvoiceAmount") AS pending_invoice_amount,
COUNT("InvoiceAmount") AS pending_invoice_count
FROM {{ ref('driveload_pending_invoices_report_source') }}
GROUP BY billing_period
ORDER BY billing_period

),

final AS (

SELECT *
FROM pending_invoice_aggregate

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-24",
updated_date="2024-03-24"
) }}

