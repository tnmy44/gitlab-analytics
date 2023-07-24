SELECT 
date_day,
pl_category,
sum(net_cost) as cost,
RATIO_TO_REPORT(SUM(net_cost)) OVER (PARTITION BY DATE_DAY) as pl_percent
FROM {{ ref('rpt_gcp_billing_pl_day_combined') }} 
where gcp_service_description = 'Compute Engine'
and (lower(gcp_sku_description) like '%ram%' 
  or lower(gcp_sku_description) like '%core%' 
 and lower(gcp_sku_description) not like '%commitment%'
 and lower(gcp_sku_description) not like '%t2d%')
and (usage_unit = 'seconds' or usage_unit = 'bytes-seconds')
and pl_category is not null
and date_trunc('month', date_day) >= '2023-02-01'
group by 1, 2
order by date_day desc