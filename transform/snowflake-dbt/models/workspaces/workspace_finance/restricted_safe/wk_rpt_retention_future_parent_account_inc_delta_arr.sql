{{ config(materialized='table') }}

{{ simple_cte([
    ('dim_date','dim_date'),
    ('dim_crm_account_daily_snapshot', 'dim_crm_account_daily_snapshot'),
    ('dim_crm_account', 'dim_crm_account'),
    ('rpt_arr', 'rpt_arr_snapshot_combined')
]) }},

dim_crm_account_live AS (
  
  SELECT
    dim_crm_account_id                       AS dim_crm_account_id_live,
    crm_account_name                         AS parent_crm_account_name_live,
    parent_crm_account_sales_segment         AS parent_crm_account_sales_segment_live,
    parent_crm_account_sales_segment_grouped AS parent_crm_account_sales_segment_grouped_live,
    parent_crm_account_geo                   AS parent_crm_account_geo_live
  FROM dim_crm_account

),

finalized_arr_months AS (

  SELECT DISTINCT
    arr_month,
    is_arr_month_finalized
  FROM rpt_arr

),

child_account_arrs AS (

  SELECT
    dim_crm_account_id                      AS child_account_id,
    dim_parent_crm_account_id               AS parent_account_id,
    arr_month,
    product_tier_name                       AS product_category,
    product_ranking                         AS product_ranking,
    {# MIN(is_arpu)                            AS is_arpu, #}
    SUM(ZEROIFNULL(mrr))                    AS mrr,
    SUM(ZEROIFNULL(arr))                    AS arr,
    SUM(ZEROIFNULL(quantity))               AS quantity
  FROM rpt_arr
  {{ dbt_utils.group_by(n=5) }}

),

py_arr_with_cy_parent AS (

  SELECT
    child_account_arrs.arr_month                                AS py_arr_month,
    DATEADD('year', 1, child_account_arrs.arr_month)            AS retention_month,
    dim_crm_account_daily_snapshot.dim_parent_crm_account_id    AS parent_account_id_in_retention_month,
    DATEADD('year', 1, dim_date.snapshot_date_fpa)              AS retention_period_snapshot_date,
    {# is_arpu                                                     AS py_is_arpu, #}
    ARRAY_AGG(product_category)                                 AS py_product_category,
    MAX(product_ranking)                                        AS py_product_ranking,
    SUM(ZEROIFNULL(child_account_arrs.mrr))                     AS py_mrr,
    SUM(ZEROIFNULL(child_account_arrs.arr))                     AS py_arr,
    SUM(ZEROIFNULL(child_account_arrs.quantity))                AS py_quantity
  FROM child_account_arrs
  LEFT JOIN dim_date
    ON child_account_arrs.arr_month::DATE = dim_date.date_day
  LEFT JOIN dim_crm_account_daily_snapshot
    ON child_account_arrs.child_account_id = dim_crm_account_daily_snapshot.dim_crm_account_id
      AND dim_crm_account_daily_snapshot.snapshot_date = DATEADD('year', 1, dim_date.snapshot_date_fpa)
  WHERE retention_month BETWEEN '2020-03-01' AND '2024-02-01'
  -- this is from when we started doing daily snapshots for dim_crm_account_daily_snapshot and used the 8th day snapshot
  {{ dbt_utils.group_by(n=4) }}

  UNION

  SELECT
    child_account_arrs.arr_month                                AS py_arr_month,
    DATEADD('year', 1, child_account_arrs.arr_month)            AS retention_month,
    dim_crm_account_daily_snapshot.dim_parent_crm_account_id    AS parent_account_id_in_retention_month,
    DATEADD('year', 1, dim_date.snapshot_date_fpa_fifth)        AS retention_period_snapshot_date,
    {# is_arpu                                                     AS py_is_arpu, #}
    ARRAY_AGG(product_category)                                 AS py_product_category,
    MAX(product_ranking)                                        AS py_product_ranking,
    SUM(ZEROIFNULL(child_account_arrs.mrr))                     AS py_mrr,
    SUM(ZEROIFNULL(child_account_arrs.arr))                     AS py_arr,
    SUM(ZEROIFNULL(child_account_arrs.quantity))                AS py_quantity
  FROM child_account_arrs
  LEFT JOIN dim_date
    ON child_account_arrs.arr_month::DATE = dim_date.date_day
  LEFT JOIN dim_crm_account_daily_snapshot
    ON child_account_arrs.child_account_id = dim_crm_account_daily_snapshot.dim_crm_account_id
      AND dim_crm_account_daily_snapshot.snapshot_date = DATEADD('year', 1, dim_date.snapshot_date_fpa_fifth)
  WHERE retention_month >= '2024-03-01'
  -- this is when we started using the 5th day snapshot
  {{ dbt_utils.group_by(n=4) }}

),

cy_arr_with_cy_parent AS (

  SELECT
    parent_account_id,
    arr_month,
    {# is_arpu                         AS retained_is_arpu, #}
    SUM(ZEROIFNULL(mrr))            AS retained_mrr,
    SUM(ZEROIFNULL(arr))            AS retained_arr,
    SUM(ZEROIFNULL(quantity))       AS retained_quantity,
    ARRAY_AGG(product_category)     AS retained_product_category,
    MAX(product_ranking)            AS retained_product_ranking
  FROM child_account_arrs
  {{ dbt_utils.group_by(n=2) }}

),

final AS (

  SELECT
    COALESCE(cy_arr_with_cy_parent.parent_account_id, py_arr_with_cy_parent.parent_account_id_in_retention_month) AS dim_parent_crm_account_id,
    finalized_arr_months.is_arr_month_finalized                                                                   AS is_arr_month_finalized,
    DATEADD('year', 1, py_arr_with_cy_parent.py_arr_month)                                                        AS retention_month,
    {{ dbt_utils.generate_surrogate_key(['dim_parent_crm_account_id', 'retention_month']) }}                      AS primary_key,
    IFF(dim_date.is_first_day_of_last_month_of_fiscal_quarter, dim_date.fiscal_quarter_name_fy, NULL)             AS retention_fiscal_quarter_name_fy,
    IFF(dim_date.is_first_day_of_last_month_of_fiscal_year, dim_date.fiscal_year, NULL)                           AS retention_fiscal_year,
    dim_crm_account_live.parent_crm_account_name_live                                                             AS parent_crm_account_name_live,
    dim_crm_account_live.parent_crm_account_sales_segment_live                                                    AS parent_crm_account_sales_segment_live,
    dim_crm_account_live.parent_crm_account_sales_segment_grouped_live                                            AS parent_crm_account_sales_segment_grouped_live,
    dim_crm_account_live.parent_crm_account_geo_live                                                              AS parent_crm_account_geo_live,
    cy_arr_with_cy_parent.retained_product_category                                                               AS net_retention_product_category,
    py_arr_with_cy_parent.py_product_category                                                                     AS prior_year_product_category,
    cy_arr_with_cy_parent.retained_product_ranking                                                                AS net_retention_product_ranking,
    py_arr_with_cy_parent.py_product_ranking                                                                      AS prior_year_product_ranking,
    {# py_arr_with_cy_parent.py_is_arpu                                                                              AS prior_year_is_arpu,
    cy_arr_with_cy_parent.retained_is_arpu                                                                        AS retention_is_arpu, #}
    CASE
      WHEN SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)) > 100000 THEN '1. ARR > $100K'
      WHEN SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)) <= 100000 AND SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)) > 5000 THEN '2. ARR $5K-100K'
      WHEN SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)) <= 5000 THEN '3. ARR <= $5K'
    END                                                                                                           AS retention_arr_band,
    SUM(ZEROIFNULL(py_arr_with_cy_parent.py_mrr))                                                                 AS prior_year_mrr,
    SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_mrr))                                                           AS net_retention_mrr,
    SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr))                                                                 AS prior_year_arr,
    SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr))                                                           AS net_retention_arr,
    CASE
      WHEN SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr)) = 0
        THEN SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr))
      ELSE 0
    END                                                                                                           AS churn_arr,
    CASE WHEN SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr)) > 0
        THEN LEAST(SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr)), SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)))
      ELSE 0 END                                                                                                  AS gross_retention_arr,
    SUM(ZEROIFNULL(py_arr_with_cy_parent.py_quantity))                                                            AS prior_year_quantity,
    SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_quantity))                                                      AS net_retention_quantity

  FROM py_arr_with_cy_parent
  LEFT JOIN cy_arr_with_cy_parent
    ON py_arr_with_cy_parent.parent_account_id_in_retention_month = cy_arr_with_cy_parent.parent_account_id
      AND py_arr_with_cy_parent.retention_month = cy_arr_with_cy_parent.arr_month
  LEFT JOIN finalized_arr_months
    ON finalized_arr_months.arr_month = py_arr_with_cy_parent.retention_month
  INNER JOIN dim_date
    ON dim_date.date_actual = py_arr_with_cy_parent.retention_month
  LEFT JOIN dim_crm_account_live
    ON dim_crm_account_live.dim_crm_account_id_live = dim_parent_crm_account_id

  {{ dbt_utils.group_by(n=14) }}

),

final_with_delta AS (
    SELECT
      final.*,
      {# CASE
        WHEN prior_year_is_arpu = 'TRUE'
          AND retention_is_arpu = 'TRUE' THEN 'Y'
        ELSE 'N'
      END                              AS is_arpu_combined, #}
      CASE
        WHEN py_arr_with_cy_parent.parent_account_id_in_retention_month IS NULL THEN 'New'
        WHEN net_retention_arr = 0
          AND prior_year_arr > 0 THEN 'Churn'
        WHEN net_retention_arr < prior_year_arr
          AND net_retention_arr > 0 THEN 'Contraction'
        WHEN net_retention_arr > prior_year_arr
          THEN 'Expansion'
        WHEN net_retention_arr = prior_year_arr THEN 'No Impact'
      END                               AS type_of_arr_change,
    CASE
      WHEN 
      {# is_arpu_combined = 'Y'
        AND  #}
        type_of_arr_change IN ('Expansion', 'Contraction')
        AND prior_year_quantity != net_retention_quantity
        AND prior_year_quantity > 0
        THEN ZEROIFNULL(
            prior_year_arr / NULLIF(prior_year_quantity, 0) * (net_retention_quantity - prior_year_quantity)
          )
      WHEN prior_year_quantity != net_retention_quantity
        AND prior_year_quantity = 0 THEN net_retention_arr
      ELSE 0
    END AS seat_change_arr,
    CASE
      WHEN 
      {# is_arpu_combined = 'N'
        AND  #}
        type_of_arr_change IN ('Expansion', 'Contraction') THEN COALESCE(net_retention_arr, 0) - COALESCE(prior_year_arr, 0)
      WHEN 
      {# is_arpu_combined = 'Y'
        AND  #}
        prior_year_product_category = net_retention_product_category
        THEN net_retention_quantity * (
            net_retention_arr / NULLIF(net_retention_quantity, 0) - prior_year_arr / NULLIF(prior_year_quantity, 0)
          )
      WHEN 
      {# is_arpu_combined = 'Y'
        AND  #}
        prior_year_product_category != net_retention_product_category
        AND prior_year_product_ranking = net_retention_product_ranking
        THEN net_retention_quantity * (
            net_retention_arr / NULLIF(net_retention_quantity, 0) - prior_year_arr / NULLIF(prior_year_quantity, 0)
          )
      ELSE 0
    END AS price_change_arr,
    CASE
      WHEN 
      {# is_arpu_combined = 'Y'
        AND  #}
        prior_year_product_ranking != net_retention_product_ranking
        THEN ZEROIFNULL(
            net_retention_quantity * (
              net_retention_arr / NULLIF(net_retention_quantity, 0) - prior_year_arr / NULLIF(prior_year_quantity, 0)
            )
          )
      ELSE 0
    END AS tier_change_arr,
    (
      CASE
        WHEN 
        {# is_arpu_combined = 'Y'
          AND  #}
          prior_year_product_ranking < net_retention_product_ranking
          THEN ZEROIFNULL(
              net_retention_quantity * (
                net_retention_arr / NULLIF(net_retention_quantity, 0) - prior_year_arr / NULLIF(prior_year_quantity, 0)
              )
            )
        ELSE 0
      END
    )   AS uptier_change_arr,
    (
      CASE
        WHEN 
        {# is_arpu_combined = 'Y'
          AND  #}
          prior_year_product_ranking > net_retention_product_ranking
          THEN ZEROIFNULL(
              net_retention_quantity * (
                net_retention_arr / NULLIF(net_retention_quantity, 0) - prior_year_quantity / NULLIF(prior_year_quantity, 0)
              )
            )
        ELSE 0
      END
    )   AS downtier_change_arr
    FROM final
    LEFT JOIN py_arr_with_cy_parent
      ON final.dim_parent_crm_account_id = py_arr_with_cy_parent.parent_account_id_in_retention_month
      AND final.retention_month = py_arr_with_cy_parent.retention_month
)

SELECT *
FROM final_with_delta
