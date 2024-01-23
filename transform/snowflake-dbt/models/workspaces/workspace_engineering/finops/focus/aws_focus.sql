WITH source as (

SELECT * FROM {{ ref('aws_billing_source')}}

)

SELECT 

DATE(line_item_usage_start_date) as date_day, --date
bill_payer_account_id as billing_account_id, -- acount id
bill_billing_period_end_date as billing_period_end, --invoice month
line_item_usage_account_id as sub_account_id, -- project.id eq
line_item_product_code as service_name,
line_item_line_item_description as charge_description, --sku desc
line_item_usage_amount as pricing_quantity, --usage amount in proicing unit
pricing_unit as pricing_unit,
line_item_net_unblended_cost as billed_cost,
pricing_public_on_demand_cost as list_cost
FROM source 

