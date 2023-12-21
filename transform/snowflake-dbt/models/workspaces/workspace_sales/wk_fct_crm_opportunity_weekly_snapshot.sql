
{{ simple_cte([
    ('sales_qualified_source', 'prep_sales_qualified_source'),
    ('order_type', 'prep_order_type'),
    ('sales_segment', 'prep_sales_segment'),
    ('prep_crm_user_hierarchy', 'prep_crm_user_hierarchy'),
    ('dim_date', 'dim_date'),
    ('fct_crm_opp', 'wk_fct_crm_opportunity_daily_snapshot'),
    ('sales_rep', 'prep_crm_user'),
    ('prep_crm_account', 'prep_crm_account')
]) }}

SELECT 
    fct_crm_opp.*, 
    prep_crm_user_hierarchy.prep_crm_user_hierarchy_sk,
    dim_date.fiscal_year,
    dim_date.fiscal_quarter_name_fy,
    dim_date.first_day_of_month,
    order_type.order_type_name,
    sales_qualified_source.sales_qualified_source_name
FROM fct_crm_opp
INNER JOIN dim_date
    ON fct_crm_opp.snapshot_date = dim_date.date_actual
LEFT JOIN prep_crm_account
  ON fct_crm_opp.dim_crm_account_id = prep_crm_account.dim_crm_account_id
LEFT JOIN sales_qualified_source
  ON fct_crm_opp.sales_qualified_source = sales_qualified_source.sales_qualified_source_name
LEFT JOIN order_type
  ON fct_crm_opp.order_type = order_type.order_type_name
LEFT JOIN sales_segment
  ON fct_crm_opp.sales_segment = sales_segment.sales_segment_name
LEFT JOIN prep_crm_user_hierarchy
  ON fct_crm_opp.dim_crm_opp_owner_stamped_hierarchy_sk = prep_crm_user_hierarchy.prep_crm_user_hierarchy_sk
LEFT JOIN prep_crm_user_hierarchy AS account_hierarchy
  ON prep_crm_account.dim_crm_parent_account_hierarchy_sk = account_hierarchy.prep_crm_user_hierarchy_sk
LEFT JOIN sales_rep
  ON fct_crm_opp.dim_crm_user_id = sales_rep.dim_crm_user_id
LEFT JOIN sales_rep AS sales_rep_account
  ON prep_crm_account.dim_crm_user_id = sales_rep_account.dim_crm_user_id
WHERE dim_date.day_of_week = 5