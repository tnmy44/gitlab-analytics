{{ simple_cte([
    ('sales_qualified_source', 'prep_sales_qualified_source'),
    ('order_type', 'prep_order_type'),
    ('sales_segment', 'prep_sales_segment'),
    ('prep_crm_user_hierarchy', 'prep_crm_user_hierarchy'),
    ('dim_date', 'dim_date'),
    ('sales_rep', 'prep_crm_user'),
    ('prep_crm_account', 'prep_crm_account'),
    ('wk_prep_crm_opportunity', 'wk_prep_crm_opportunity')
]) }},

 final AS (

  SELECT {{ dbt_utils.star(from=ref('wk_prep_crm_opportunity'), 
    except=["CREATED_BY", "UPDATED_BY", "MODEL_CREATED_DATE", "MODEL_UPDATED_DATE", "DBT_UPDATED_AT", "DBT_CREATED_AT", "IS_NET_ARR_PIPELINE_CREATED", "IS_ELIGIBLE_OPEN_PIPELINE", "IS_EXCLUDED_FROM_PIPELINE_CREATED", "CREATED_AND_WON_SAME_QUARTER_NET_ARR", "IS_ELIGIBLE_AGE_ANALYSIS", "CYCLE_TIME_IN_DAYS"]) }},
    dim_date.fiscal_year,
    dim_date.fiscal_quarter_name_fy,
    dim_date.first_day_of_month,
    {{ get_keyed_nulls('sales_qualified_source.dim_sales_qualified_source_id') }}                                               AS dim_sales_qualified_source_id,
    {{ get_keyed_nulls('order_type.dim_order_type_id') }}                                                                       AS dim_order_type_id,
    {{ get_keyed_nulls('sales_rep_account.dim_crm_user_hierarchy_sk') }}                                                        AS dim_crm_user_hierarchy_sk
  FROM wk_prep_crm_opportunity
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
  WHERE is_live = 0
  {% if is_incremental() %}
  
    AND snapshot_date > (SELECT MAX(snapshot_date) FROM {{this}})

  {% endif %}


)

SELECT * 
FROM final