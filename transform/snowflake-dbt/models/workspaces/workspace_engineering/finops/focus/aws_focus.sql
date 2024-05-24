WITH source AS (

  SELECT *
  FROM {{ ref('aws_billing_source')}}

),

unique_ids AS (

  SELECT DISTINCT
    bill_payer_account_id,
    identity_line_item_id,
    identity_time_interval
  FROM source
),

filtered as (

  SELECT source.*
  FROM source
  INNER JOIN unique_ids ON source.bill_payer_account_id = unique_ids.bill_payer_account_id
  AND source.identity_line_item_id = unique_ids.identity_line_item_id
    AND source.identity_time_interval = unique_ids.identity_time_interval

)

SELECT
  DATE(line_item_usage_start_date) AS date_day, --date
  bill_payer_account_id AS billing_account_id, -- acount id  
  bill_billing_period_end_date AS billing_period_end, --invoice month
  line_item_usage_account_id AS sub_account_id, -- project.id eq
  line_item_product_code AS service_name,
  line_item_line_item_description AS charge_description, --sku desc
  line_item_usage_amount AS pricing_quantity, --usage amount in proicing unit
  pricing_unit AS pricing_unit,
  line_item_net_unblended_cost AS billed_cost,
  pricing_public_on_demand_cost AS list_cost
FROM filtered

