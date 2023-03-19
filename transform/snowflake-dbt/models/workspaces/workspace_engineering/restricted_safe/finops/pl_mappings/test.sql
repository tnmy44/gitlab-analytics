SELECT sum(net_cost) FROM {{ ref ('rpt_gcp_billing_pl_day') }} where from_mapping = 'container_registry_pl_daily' and date_trunc('month', date_day) = '2023-02-01' 



SELECT * FROM {{ ref ('combined_pl_mapping') }} where from_mapping = 'single_sku_pl' and date_trunc('month', date_day) = '2023-02-01' 