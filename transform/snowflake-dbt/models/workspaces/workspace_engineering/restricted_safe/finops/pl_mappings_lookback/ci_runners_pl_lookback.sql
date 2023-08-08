SELECT
  date_day,
  pl_category,
  SUM(net_cost)                                               AS cost,
  RATIO_TO_REPORT(SUM(net_cost)) OVER (PARTITION BY date_day) AS pl_percent
FROM {{ ref('rpt_gcp_billing_pl_day_combined') }}
WHERE from_mapping LIKE '%ci_runner_pl_daily%'
GROUP BY 1, 2
