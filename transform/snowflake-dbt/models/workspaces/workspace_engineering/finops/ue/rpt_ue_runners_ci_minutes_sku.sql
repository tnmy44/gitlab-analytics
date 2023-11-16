WITH cost_data AS (

    SELECT * FROM {{ ref ('rpt_gcp_billing_pl_day_ext')}}

)

SELECT date_day,
    gcp_sku_description,
    pricing_unit,
    pl_category,
    level_3,
    level_4,
    sum(usage_amount_in_pricing_units) as usage_amount_in_pricing_units,
    sum(net_cost) as net_cost
FROM cost_data
where level_4 IN ('linux small', 'linux medium', 'linux large', 'linux xlarge gpu', 'linux medium gpu', 'linux private internal')
and gcp_sku_description like 'N2D AMD Instance Core running in%'
and date_day >= '2023-02-01'
group by 1,2,3,4,5,6
order by 1 desc