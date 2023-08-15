use warehouse reporting;

select
    record_type,
    count(*)
from raw.sales_analytics.tableau_asm_consolidated_sources
group by 1

use warehouse reporting;
SELECT *
FROM PROD.restricted_safe_workspace_sales.rsa_tableau_asm_consolidated
limit 1

SELECT distinct snapshot_date
FROM PROD.restricted_safe_workspace_sales.rsa_tableau_asm_consolidated
where record_type = 'opportunity snapshot cq open closed agg'

SELECT sum(net_arr)
FROM PROD.restricted_safe_workspace_sales.rsa_tableau_asm_consolidated
WHERE record_type = 'net arr aggregated'
and close_fiscal_quarter_name = 'FY24-Q2'
and is_open_stage_1_plus = True
--and is_open = 1


SELECT close_fiscal_quarter_name,
       sum(net_arr)
FROM PROD.restricted_safe_workspace_sales.rsa_tableau_asm_consolidated
WHERE is_open = 1
AND record_type = 'opportunity snapshot cq open closed agg'
group by 1
