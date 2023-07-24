SELECT date_day,
pl_category,
sum(net_cost) as cost,
RATIO_TO_REPORT(SUM(net_cost)) OVER (PARTITION BY DATE_DAY) as pl_percent
FROM {{ ref('rpt_gcp_billing_pl_day_combined') }} 
where from_mapping like '%ci_runner_pl_daily%'
group by 1,2
