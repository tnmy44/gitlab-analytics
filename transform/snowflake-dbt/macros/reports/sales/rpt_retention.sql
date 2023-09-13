{%- macro rpt_retention(fields) -%}

{% set fields_length = fields|length %}
{% set surrogate_key_array = [] %}

{% for field in fields %}
    {% set new_field = 'retention_subs.' + field %}
    {% do surrogate_key_array.append(new_field) %}
{% endfor %}
{% do surrogate_key_array.append('retention_month') %}


{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_crm_account', 'dim_crm_account'),
    ('dim_product_detail', 'dim_product_detail'),
    ('dim_subscription', 'dim_subscription')
]) }}

, rpt_arr AS (

    SELECT *
    FROM {{ ref('rpt_arr_snapshot_combined_8th_calendar_day') }}

)
{# , next_renewal_month AS (

    SELECT DISTINCT
      merged_accounts.dim_parent_crm_account_id,
      MIN(subscription_end_month) OVER (PARTITION BY merged_accounts.dim_parent_crm_account_id)    AS next_renewal_month
    FROM fct_mrr
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    LEFT JOIN dim_crm_account AS crm_accounts
      ON crm_accounts.dim_crm_account_id = fct_mrr.dim_crm_account_id
    INNER JOIN dim_crm_account AS merged_accounts
      ON merged_accounts.dim_crm_account_id = COALESCE(crm_accounts.merged_to_account_id, crm_accounts.dim_crm_account_id)
    LEFT JOIN dim_subscription
      ON dim_subscription.dim_subscription_id = fct_mrr.dim_subscription_id
      AND subscription_end_month <= DATEADD('year', 1, date_actual)
    WHERE subscription_end_month >= DATE_TRUNC('month',CURRENT_DATE)

), last_renewal_month AS (

    SELECT DISTINCT
      merged_accounts.dim_parent_crm_account_id,
      MAX(subscription_end_month) OVER (PARTITION BY merged_accounts.dim_parent_crm_account_id)    AS last_renewal_month
    FROM fct_mrr
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    LEFT JOIN dim_crm_account AS crm_accounts
      ON crm_accounts.dim_crm_account_id = fct_mrr.dim_crm_account_id
    INNER JOIN dim_crm_account AS merged_accounts
      ON merged_accounts.dim_crm_account_id = COALESCE(crm_accounts.merged_to_account_id, crm_accounts.dim_crm_account_id)
    LEFT JOIN dim_subscription
      ON dim_subscription.dim_subscription_id = fct_mrr.dim_subscription_id
      AND subscription_end_month <= DATEADD('year', 1, date_actual)
    WHERE subscription_end_month < DATE_TRUNC('month',CURRENT_DATE)

) #}
, parent_account_mrrs AS (

    SELECT
      {% for field in fields %}

        {%- if field == 'dim_parent_crm_account_id' -%}
          dim_crm_account.{{field}},
        {%- else -%}
          rpt_arr.{{field}},
        {%- endif -%}

      {% endfor %}

      rpt_arr.is_arr_month_finalized,
      rpt_arr.arr_month                                         AS arr_month,
      DATEADD('year', 1, rpt_arr.arr_month)                     AS retention_month,
      {# next_renewal_month,
      last_renewal_month, #}
      COUNT(DISTINCT dim_crm_account.dim_parent_crm_account_id)
                                                                AS parent_customer_count,
      SUM(ZEROIFNULL(rpt_arr.mrr))                              AS mrr_total,
      SUM(ZEROIFNULL(rpt_arr.arr))                              AS arr_total,
      SUM(ZEROIFNULL(rpt_arr.quantity))                         AS quantity_total,
      ARRAY_AGG(rpt_arr.product_tier_name)                      AS product_category,
      MAX(rpt_arr.product_ranking)                              AS product_ranking
    FROM rpt_arr
    {# INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = rpt_arr.dim_product_detail_id #}
    LEFT JOIN dim_crm_account
      ON dim_crm_account.dim_crm_account_id = rpt_arr.dim_crm_account_id
    {# LEFT JOIN next_renewal_month
      ON next_renewal_month.dim_parent_crm_account_id = dim_crm_account.dim_parent_crm_account_id
    LEFT JOIN last_renewal_month
      ON last_renewal_month.dim_parent_crm_account_id = dim_crm_account.dim_parent_crm_account_id #}
    WHERE dim_crm_account.is_jihu_account != 'TRUE'
    {{ dbt_utils.group_by(n=3 + fields_length) }}

), retention_subs AS (

    SELECT
      {% for field in fields %}
        current_mrr.{{field}},
      {% endfor %}
      future_mrr.is_arr_month_finalized AS is_arr_month_finalized,
      current_mrr.arr_month             AS current_arr_month,
      current_mrr.retention_month,
      current_mrr.mrr_total             AS current_mrr,
      future_mrr.mrr_total              AS future_mrr,
      current_mrr.arr_total             AS current_arr,
      future_mrr.arr_total              AS future_arr,
      current_mrr.parent_customer_count AS current_parent_customer_count,
      future_mrr.parent_customer_count  AS future_parent_customer_count,
      current_mrr.quantity_total        AS current_quantity,
      future_mrr.quantity_total         AS future_quantity,
      current_mrr.product_category      AS current_product_category,
      future_mrr.product_category       AS future_product_category,
      current_mrr.product_ranking       AS current_product_ranking,
      future_mrr.product_ranking        AS future_product_ranking,
      {# current_mrr.last_renewal_month,
      current_mrr.next_renewal_month, #}
      --The type of arr change requires a row_number. Row_number = 1 indicates new in the macro; however, for retention, new is not a valid option since retention starts in month 12, well after the First Order transaction.
      2                              AS row_number
    FROM parent_account_mrrs AS current_mrr
    LEFT JOIN parent_account_mrrs AS future_mrr
      ON current_mrr.dim_parent_crm_account_id = future_mrr.dim_parent_crm_account_id
      AND current_mrr.retention_month = future_mrr.arr_month

), final AS (

    SELECT
    {{ dbt_utils.surrogate_key(surrogate_key_array) }} AS primary_key,
      {% for field in fields %}
        retention_subs.{{field}}                AS {{field}},
      {% endfor %}
      dim_crm_account.crm_account_name          AS parent_crm_account_name,
      retention_subs.is_arr_month_finalized,
      retention_subs.retention_month,
      IFF(is_first_day_of_last_month_of_fiscal_year, fiscal_year, NULL)               AS retention_fiscal_year,
      IFF(is_first_day_of_last_month_of_fiscal_quarter, fiscal_quarter_name_fy, NULL) AS retention_fiscal_quarter,
      {# retention_subs.last_renewal_month,
      retention_subs.next_renewal_month, #}
      retention_subs.current_mrr                AS prior_year_mrr,
      COALESCE(future_mrr, 0)                   AS net_retention_mrr,
      CASE WHEN net_retention_mrr > 0
        THEN LEAST(net_retention_mrr, retention_subs.current_mrr)
        ELSE 0 END                              AS gross_retention_mrr,
      retention_subs.current_arr                AS prior_year_arr,
      COALESCE(retention_subs.future_arr, 0)    AS net_retention_arr,
      CASE WHEN net_retention_arr > 0
        THEN least(net_retention_arr, retention_subs.current_arr)
        ELSE 0 END                              AS gross_retention_arr,
      retention_subs.current_quantity                AS prior_year_quantity,
      COALESCE(retention_subs.future_quantity, 0)   AS net_retention_quantity,
      retention_subs.current_parent_customer_count             AS prior_year_parent_customer_count,
      COALESCE(retention_subs.future_parent_customer_count, 0) AS net_retention_parent_customer_count,
      {{ reason_for_quantity_change_seat_change('net_retention_quantity', 'prior_year_quantity') }},
      retention_subs.future_product_category                   AS net_retention_product_category,
      retention_subs.current_product_category                  AS prior_year_product_category,
      retention_subs.future_product_ranking                    AS net_retention_product_ranking,
      retention_subs.current_product_ranking                   AS prior_year_product_ranking
    FROM retention_subs
    INNER JOIN dim_date
      ON dim_date.date_actual = retention_subs.retention_month
    LEFT JOIN dim_crm_account
      ON dim_crm_account.dim_crm_account_id = retention_subs.dim_parent_crm_account_id
    WHERE retention_month <= DATE_TRUNC('month', CURRENT_DATE::DATE)

)

SELECT *
FROM final

{%- endmacro %}
