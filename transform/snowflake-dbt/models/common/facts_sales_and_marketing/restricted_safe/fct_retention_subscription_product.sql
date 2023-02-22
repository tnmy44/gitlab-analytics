{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_crm_account', 'dim_crm_account'),
    ('dim_product_detail', 'dim_product_detail'),
    ('dim_subscription', 'dim_subscription')
]) }}

, fct_mrr AS (

    SELECT *
    FROM {{ ref('fct_mrr') }}
    WHERE subscription_status IN ('Active', 'Cancelled')

), fct_arr_agg AS (

    SELECT 
      fct_mrr.dim_subscription_id,
      fct_mrr.dim_crm_account_id,
      dim_subscription.subscription_name,
      dim_subscription.oldest_subscription_in_cohort,
      dim_subscription.subscription_lineage,
      fct_mrr.arr_month,
      sum(arr) AS arr
    FROM fct_mrr
    LEFT JOIN dim_subscription
      ON fct_mrr.dim_subscription_id = dim_subscription.dim_subscription_id
    {{ dbt_utils.group_by(n=6) }}

), list AS ( --get all the subscription + their lineage + the month we're looking for MRR for (12 month in the future)

    SELECT 
      subscription_name  AS original_sub,
      c.value::VARCHAR  AS subscriptions_in_lineage,
      arr_month  AS original_arr_month,
      DATEADD('year', 1, arr_month) AS retention_month
    FROM fct_arr_agg,
    lateral flatten(input =>split(subscription_lineage, ',')) C
    {{ dbt_utils.group_by(n=4) }}

), retention_subs AS ( --find which of those subscriptions are real and group them by their sub you're comparing to.

    SELECT 
      original_sub,
      retention_month,
      original_mrr_month,
      sum(arr) AS retention_arr
    FROM list
    INNER JOIN fct_arr_agg AS subs
      ON retention_month = arr_month
      AND subscriptions_in_lineage = subscription_name
    {{ dbt_utils.group_by(n=3) }}

), final AS (

    SELECT 
      fct_arr_agg.*,
      COALESCE(retention_subs.retention_arr, 0) AS net_retention_arr,
      CASE 
        WHEN net_retention_arr > 0 
          THEN least(net_retention_mrr, mrr)
          ELSE 0 
        END AS gross_retention_arr,
      retention_month
    FROM fct_arr_agg
    LEFT JOIN retention_subs
      ON subscription_name = original_sub
      AND retention_subs.original_arr_month = fct_arr_agg.arr_month

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2023-02-22",
    updated_date="2023-02-22"
) }}
