{{ simple_cte([
    ('fct_invoice_item', 'fct_invoice_item'),
    ('dim_date', 'dim_date'),
    ('dim_crm_account', 'dim_crm_account'),
    ('dim_product_detail', 'dim_product_detail')  
]) }},

invoice_detail AS (

  SELECT
    invoice_number,
    invoice_item_id,
    dim_crm_account_id_invoice,
    dim_crm_account_id_subscription,
    dim_crm_account.crm_account_name,
    crm_account_type,
    invoice_date,
    dim_product_detail.product_name,
    dim_product_detail.product_rate_plan_name,
    quantity,
    invoice_item_unit_price,
    invoice_item_charge_amount,
    annual_billing_list_price                           AS annual_price,
    annual_price * quantity                             AS quantity_times_annual,
    parent_crm_account_sales_segment,
    NULL                                                AS product_category,
    dim_date.first_day_of_month                         AS invoice_month,
    dim_date.fiscal_quarter_name_fy,
    dim_product_detail.billing_list_price,
    NULL                                                AS discount,
    dim_product_detail.billing_list_price * quantity    AS list_price_times_quantity
  FROM fct_invoice_item
    INNER JOIN dim_date ON fct_invoice_item.invoice_date = dim_date.date_actual
    LEFT JOIN dim_crm_account ON fct_invoice_item.dim_crm_account_id_invoice = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_product_detail ON fct_invoice_item.dim_product_detail_id = dim_product_detail.dim_product_detail_id
)

SELECT *
FROM invoice_detail
