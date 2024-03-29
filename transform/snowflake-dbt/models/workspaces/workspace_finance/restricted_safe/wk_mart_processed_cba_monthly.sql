WITH increase_cba AS

(

SELECT 
DATE(DATE_TRUNC('month', WK_FINANCE_FCT_CREDIT_BALANCE_ADJUSTMENT.CREDIT_BALANCE_ADJUSTMENT_DATE)) AS cba_increase_date,
SUM(WK_FINANCE_FCT_CREDIT_BALANCE_ADJUSTMENT.CREDIT_BALANCE_ADJUSTMENT_AMOUNT) AS increase
FROM PROD.RESTRICTED_SAFE_WORKSPACE_FINANCE.WK_FINANCE_FCT_CREDIT_BALANCE_ADJUSTMENT
WHERE WK_FINANCE_FCT_CREDIT_BALANCE_ADJUSTMENT.CREDIT_BALANCE_ADJUSTMENT_TYPE = 'Increase'
AND WK_FINANCE_FCT_CREDIT_BALANCE_ADJUSTMENT.CREDIT_BALANCE_ADJUSTMENT_STATUS = 'Processed'
GROUP BY cba_increase_date
ORDER BY cba_increase_date

),

decrease_cba AS

(

SELECT
DATE(DATE_TRUNC('month', WK_FINANCE_FCT_CREDIT_BALANCE_ADJUSTMENT.CREDIT_BALANCE_ADJUSTMENT_DATE)) AS cba_decrease_date,
SUM(WK_FINANCE_FCT_CREDIT_BALANCE_ADJUSTMENT.CREDIT_BALANCE_ADJUSTMENT_AMOUNT) AS decrease
FROM PROD.RESTRICTED_SAFE_WORKSPACE_FINANCE.WK_FINANCE_FCT_CREDIT_BALANCE_ADJUSTMENT
WHERE WK_FINANCE_FCT_CREDIT_BALANCE_ADJUSTMENT.CREDIT_BALANCE_ADJUSTMENT_TYPE = 'Decrease'
AND WK_FINANCE_FCT_CREDIT_BALANCE_ADJUSTMENT.CREDIT_BALANCE_ADJUSTMENT_STATUS = 'Processed'
GROUP BY cba_decrease_date
ORDER BY cba_decrease_date

),

final AS

(

SELECT 
COALESCE(increase_cba.cba_increase_date, decrease_cba.cba_decrease_date) AS cba_date,
COALESCE(increase_cba.increase,0) AS increase,
COALESCE(decrease_cba.decrease,0) AS decrease,
SUM(increase_cba.increase - decrease_cba.decrease) OVER (ORDER BY cba_date) AS credit_balance_per_month
FROM increase_cba
FULL OUTER JOIN decrease_cba ON decrease_cba.cba_decrease_date = increase_cba.cba_increase_date
ORDER BY cba_date

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-29",
updated_date="2024-03-29"
) }}

