{{ simple_cte([
    ('dim_billing_account','dim_billing_account'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_date','dim_date'),
    ('dim_product_detail','dim_product_detail'),
    ('dim_subscription','dim_subscription'),
    ('fct_mrr', 'fct_mrr')
]) }}

, oldest_subscription_in_cohort AS (

    SELECT 
      sub.dim_subscription_id,
      sub_w_org_id.dim_subscription_id AS dim_oldest_subscription_in_cohort_id,
      sub_w_org_id.dim_crm_account_id AS dim_oldest_crm_account_in_cohort_id
    FROM dim_subscription sub
    LEFT JOIN dim_subscription sub_w_org_id
      ON sub.oldest_subscription_in_cohort = sub_w_org_id.subscription_name_slugify
    WHERE sub.subscription_status IN ('Active', 'Cancelled')
      AND sub_w_org_id.subscription_status IN ('Active', 'Cancelled')

)

, mart_arr AS (

    SELECT
      dim_date.date_actual                                                                          AS arr_month,
      IFF(is_first_day_of_last_month_of_fiscal_quarter, fiscal_quarter_name_fy, NULL)               AS fiscal_quarter_name_fy,
      IFF(is_first_day_of_last_month_of_fiscal_year, fiscal_year, NULL)                             AS fiscal_year,
      dim_crm_account.dim_parent_crm_account_id,
      COALESCE(dim_crm_account.merged_to_account_id, dim_crm_account.dim_crm_account_id)            AS dim_crm_account_id,
      oldest_subscription_in_cohort.dim_oldest_subscription_in_cohort_id,
      dim_product_detail.product_tier_name,
      dim_product_detail.product_delivery_type,
      dim_product_detail.product_ranking,
      fct_mrr.mrr,
      fct_mrr.quantity
    FROM fct_mrr
    INNER JOIN oldest_subscription_in_cohort
      ON oldest_subscription_in_cohort.dim_subscription_id = fct_mrr.dim_subscription_id
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id    
    LEFT JOIN dim_crm_account
      ON oldest_subscription_in_cohort.dim_oldest_crm_account_in_cohort_id = dim_crm_account.dim_crm_account_id
    WHERE fct_mrr.subscription_status IN ('Active', 'Cancelled')
      AND dim_crm_account.is_jihu_account != 'TRUE'

), max_min_month AS (

    SELECT
      dim_parent_crm_account_id,
      dim_crm_account_id,
      dim_oldest_subscription_in_cohort_id,
      MIN(arr_month)                      AS date_month_start,
      --add 1 month to generate churn month
      DATEADD('month',1,MAX(arr_month))   AS date_month_end
    FROM mart_arr
    {{ dbt_utils.group_by(n=3) }}

), base AS (

    SELECT
      dim_parent_crm_account_id,
      dim_crm_account_id,
      dim_oldest_subscription_in_cohort_id,
      dim_date.date_actual         AS arr_month,
      dim_date.fiscal_quarter_name_fy,
      dim_date.fiscal_year
    FROM max_min_month
    INNER JOIN dim_date
      -- all months after start date
      ON  dim_date.date_actual >= max_min_month.date_month_start
      -- up to and including end date
      AND dim_date.date_actual <=  max_min_month.date_month_end
      AND day_of_month = 1

), monthly_arr_subscription_level AS (

    SELECT
      base.arr_month,
      base.dim_parent_crm_account_id,
      base.dim_crm_account_id,
      base.dim_oldest_subscription_in_cohort_id,
      ARRAY_AGG(DISTINCT product_tier_name ) WITHIN GROUP (ORDER BY product_tier_name  ASC)   AS product_tier_name,
      ARRAY_AGG(DISTINCT product_delivery_type) WITHIN GROUP (ORDER BY product_delivery_type ASC) AS product_delivery_type,
      MAX(product_ranking)                                                                    AS product_ranking,
      SUM(ZEROIFNULL(quantity))                                                               AS quantity,
      SUM(ZEROIFNULL(mrr)*12)                                                                 AS arr
    FROM base
    LEFT JOIN mart_arr
      ON base.arr_month = mart_arr.arr_month
      AND base.dim_oldest_subscription_in_cohort_id = mart_arr.dim_oldest_subscription_in_cohort_id
    {{ dbt_utils.group_by(n=4) }}

), prior_month AS (

    SELECT
      monthly_arr_subscription_level.*,
      LAG(product_tier_name ) OVER (PARTITION BY dim_oldest_subscription_in_cohort_id ORDER BY arr_month) AS previous_product_tier_name ,
      LAG(product_delivery_type) OVER (PARTITION BY dim_oldest_subscription_in_cohort_id ORDER BY arr_month) AS previous_product_delivery_type,
      COALESCE(LAG(product_ranking) OVER (PARTITION BY dim_oldest_subscription_in_cohort_id ORDER BY arr_month),0) AS previous_product_ranking,
      COALESCE(LAG(quantity) OVER (PARTITION BY dim_oldest_subscription_in_cohort_id ORDER BY arr_month),0) AS previous_quantity,
      COALESCE(LAG(arr) OVER (PARTITION BY dim_oldest_subscription_in_cohort_id ORDER BY arr_month),0) AS previous_arr,
      ROW_NUMBER() OVER (PARTITION BY dim_oldest_subscription_in_cohort_id ORDER BY arr_month) AS row_number
    FROM monthly_arr_subscription_level

), type_of_arr_change AS (

    SELECT
      prior_month.*,
      {{ type_of_arr_change('arr','previous_arr','row_number') }}
    FROM prior_month

), reason_for_arr_change_beg AS (

    SELECT
      arr_month,
      dim_oldest_subscription_in_cohort_id,
      previous_arr      AS beg_arr,
      previous_quantity AS beg_quantity
    FROM type_of_arr_change

), reason_for_arr_change_seat_change AS (

    SELECT
      arr_month,
      dim_oldest_subscription_in_cohort_id,
      {{ reason_for_arr_change_seat_change('quantity', 'previous_quantity', 'arr', 'previous_arr') }},
      {{ reason_for_quantity_change_seat_change('quantity', 'previous_quantity') }}
    FROM type_of_arr_change

), reason_for_arr_change_price_change AS (

    SELECT
      arr_month,
      dim_oldest_subscription_in_cohort_id,
      {{ reason_for_arr_change_price_change('product_tier_name ', 'previous_product_tier_name ', 'quantity', 'previous_quantity', 'arr', 'previous_arr', 'product_ranking',' previous_product_ranking') }}
    FROM type_of_arr_change

), reason_for_arr_change_tier_change AS (

    SELECT
      arr_month,
      dim_oldest_subscription_in_cohort_id,
      {{ reason_for_arr_change_tier_change('product_ranking', 'previous_product_ranking', 'quantity', 'previous_quantity', 'arr', 'previous_arr') }}
    FROM type_of_arr_change

), reason_for_arr_change_end AS (

    SELECT
      arr_month,
      dim_oldest_subscription_in_cohort_id,
      arr                   AS end_arr,
      quantity              AS end_quantity
    FROM type_of_arr_change

), annual_price_per_seat_change AS (

    SELECT
      arr_month,
      dim_oldest_subscription_in_cohort_id,
      {{ annual_price_per_seat_change('quantity', 'previous_quantity', 'arr', 'previous_arr') }}
    FROM type_of_arr_change

), combined AS (

    SELECT
      {{ dbt_utils.surrogate_key(['type_of_arr_change.arr_month', 'type_of_arr_change.dim_oldest_subscription_in_cohort_id']) }}
                                                                    AS delta_arr_subscription_month_pk,
      type_of_arr_change.arr_month,
      --type_of_arr_change.dim_parent_crm_account_id,
      --type_of_arr_change.dim_crm_account_id,
      type_of_arr_change.dim_oldest_subscription_in_cohort_id,
      type_of_arr_change.product_tier_name ,
      type_of_arr_change.previous_product_tier_name AS previous_month_product_tier_name ,
      type_of_arr_change.product_delivery_type,
      type_of_arr_change.previous_product_delivery_type AS previous_month_product_delivery_type,
      type_of_arr_change.product_ranking,
      type_of_arr_change.previous_product_ranking AS previous_month_product_ranking,
      type_of_arr_change.type_of_arr_change,
      reason_for_arr_change_beg.beg_arr,
      reason_for_arr_change_beg.beg_quantity,
      reason_for_arr_change_seat_change.seat_change_arr,
      reason_for_arr_change_seat_change.seat_change_quantity,
      reason_for_arr_change_price_change.price_change_arr,
      reason_for_arr_change_tier_change.tier_change_arr,
      reason_for_arr_change_end.end_arr,
      reason_for_arr_change_end.end_quantity,
      annual_price_per_seat_change.annual_price_per_seat_change
    FROM type_of_arr_change
    LEFT JOIN reason_for_arr_change_beg
      ON type_of_arr_change.dim_oldest_subscription_in_cohort_id = reason_for_arr_change_beg.dim_oldest_subscription_in_cohort_id
      AND type_of_arr_change.arr_month = reason_for_arr_change_beg.arr_month
    LEFT JOIN reason_for_arr_change_seat_change
      ON type_of_arr_change.dim_oldest_subscription_in_cohort_id = reason_for_arr_change_seat_change.dim_oldest_subscription_in_cohort_id
      AND type_of_arr_change.arr_month = reason_for_arr_change_seat_change.arr_month
    LEFT JOIN reason_for_arr_change_price_change
      ON type_of_arr_change.dim_oldest_subscription_in_cohort_id = reason_for_arr_change_price_change.dim_oldest_subscription_in_cohort_id
      AND type_of_arr_change.arr_month = reason_for_arr_change_price_change.arr_month
    LEFT JOIN reason_for_arr_change_tier_change
      ON type_of_arr_change.dim_oldest_subscription_in_cohort_id = reason_for_arr_change_tier_change.dim_oldest_subscription_in_cohort_id
      AND type_of_arr_change.arr_month = reason_for_arr_change_tier_change.arr_month
    LEFT JOIN reason_for_arr_change_end
      ON type_of_arr_change.dim_oldest_subscription_in_cohort_id = reason_for_arr_change_end.dim_oldest_subscription_in_cohort_id
      AND type_of_arr_change.arr_month = reason_for_arr_change_end.arr_month
    LEFT JOIN annual_price_per_seat_change
      ON type_of_arr_change.dim_oldest_subscription_in_cohort_id = annual_price_per_seat_change.dim_oldest_subscription_in_cohort_id
      AND type_of_arr_change.arr_month = annual_price_per_seat_change.arr_month

)

{{ dbt_audit(
    cte_ref="combined",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2023-02-22",
    updated_date="2023-02-22"
) }}
