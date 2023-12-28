WITH dim_date AS (

  SELECT * 
  FROM {{ref('dim_date')}}

),


targets AS (

  SELECT *
  FROM {{ref('wk_fct_sales_funnel_target_daily_pivoted')}}

),

actuals AS (

  SELECT *
  FROM {{ref('wk_fct_crm_opportunity_daily_snapshot')}}

),

combined AS (

  SELECT 
    actuals.*,
    -- "Churn % ATR Daily",
    -- "Churn % ATR Monthly",
    -- "Churn % ATR WTD",
    -- "Churn % ATR MTD",
    -- "Churn % ATR QTD",
    -- "Churn % ATR YTD",
    -- "Churn Amount Daily",
    -- "Churn Amount Monthly",
    -- "Churn Amount WTD",
    -- "Churn Amount MTD",
    -- "Churn Amount QTD",
    -- "Churn Amount YTD",
    -- "Churn/Contraction Amount Daily",
    -- "Churn/Contraction Amount Monthly",
    -- "Churn/Contraction Amount WTD",
    -- "Churn/Contraction Amount MTD",
    -- "Churn/Contraction Amount QTD",
    -- "Churn/Contraction Amount YTD",
    -- "Contraction % ATR Daily",
    -- "Contraction % ATR Monthly",
    -- "Contraction % ATR WTD",
    -- "Contraction % ATR MTD",
    -- "Contraction % ATR QTD",
    -- "Contraction % ATR YTD",
    -- "Contraction Amount Daily",
    -- "Contraction Amount Monthly",
    -- "Contraction Amount WTD",
    -- "Contraction Amount MTD",
    -- "Contraction Amount QTD",
    -- "Contraction Amount YTD",
    deals."Deals Daily",
    deals."Deals Monthly",
    deals."Deals WTD",
    deals."Deals MTD",
    deals."Deals QTD",
    deals."Deals YTD",
    -- "Expansion % ATR Daily",
    -- "Expansion % ATR Monthly",
    -- "Expansion % ATR WTD",
    -- "Expansion % ATR MTD",
    -- "Expansion % ATR QTD",
    -- "Expansion % ATR YTD",
    -- "Expansion Amount Daily",
    -- "Expansion Amount Monthly",
    -- "Expansion Amount WTD",
    -- "Expansion Amount MTD",
    -- "Expansion Amount QTD",
    -- "Expansion Amount YTD",
    -- "High LAM Stage Closed Won Opportunities Daily",
    -- "High LAM Stage Closed Won Opportunities Monthly",
    -- "High LAM Stage Closed Won Opportunities WTD",
    -- "High LAM Stage Closed Won Opportunities MTD",
    -- "High LAM Stage Closed Won Opportunities QTD",
    -- "High LAM Stage Closed Won Opportunities YTD",
    -- "MQL Daily",
    -- "MQL Monthly",
    -- "MQL WTD",
    -- "MQL MTD",
    -- "MQL QTD",
    -- "MQL YTD",
    net_arr."Net ARR Daily",
    net_arr."Net ARR Monthly",
    net_arr."Net ARR WTD",
    net_arr."Net ARR MTD",
    net_arr."Net ARR QTD",
    net_arr."Net ARR YTD",
    net_arr."Net ARR Company Daily",
    net_arr."Net ARR Company Monthly",
    net_arr."Net ARR Company WTD",
    net_arr."Net ARR Company MTD",
    net_arr."Net ARR Company QTD",
    net_arr."Net ARR Company YTD",
    net_arr_pipeline_created."Net ARR Pipeline Created Daily",
    net_arr_pipeline_created."Net ARR Pipeline Created Monthly",
    net_arr_pipeline_created."Net ARR Pipeline Created WTD",
    net_arr_pipeline_created."Net ARR Pipeline Created MTD",
    net_arr_pipeline_created."Net ARR Pipeline Created QTD",
    net_arr_pipeline_created."Net ARR Pipeline Created YTD",
    new_logos."New Logos Daily",
    new_logos."New Logos Monthly",
    new_logos."New Logos WTD",
    new_logos."New Logos MTD",
    new_logos."New Logos QTD",
    new_logos."New Logos YTD",
    -- "ProServe Amount Daily",
    -- "ProServe Amount Monthly",
    -- "ProServe Amount WTD",
    -- "ProServe Amount MTD",
    -- "ProServe Amount QTD",
    -- "ProServe Amount YTD",
    stage_1."Stage 1 Opportunities Daily",
    stage_1."Stage 1 Opportunities Monthly",
    stage_1."Stage 1 Opportunities WTD",
    stage_1."Stage 1 Opportunities MTD",
    stage_1."Stage 1 Opportunities QTD",
    stage_1."Stage 1 Opportunities YTD",
    total_closed."Total Closed Daily",
    total_closed."Total Closed Monthly",
    total_closed."Total Closed WTD",
    total_closed."Total Closed MTD",
    total_closed."Total Closed QTD",
    total_closed."Total Closed YTD"
    -- "Trials Daily",
    -- "Trials Monthly",
    -- "Trials WTD",
    -- "Trials MTD",
    -- "Trials QTD",
    -- "Trials YTD"
  FROM actuals
  LEFT JOIN targets AS net_arr
    ON net_arr.actuals_targets_daily_pk = actuals.actuals_targets_daily_pk
  LEFT JOIN targets AS deals
    ON deals.actuals_targets_daily_pk = actuals.actuals_targets_daily_pk
  LEFT JOIN targets AS net_arr_pipeline_created
    ON net_arr_pipeline_created.actuals_targets_daily_pk = actuals.actuals_targets_daily_pk
  LEFT JOIN targets AS total_closed
    ON total_closed.actuals_targets_daily_pk = actuals.actuals_targets_daily_pk
  LEFT JOIN targets AS stage_1
    ON stage_1.actuals_targets_daily_pk = actuals.actuals_targets_daily_pk
  LEFT JOIN targets AS new_logos
    ON new_logos.actuals_targets_daily_pk = actuals.actuals_targets_daily_pk

)

SELECT * 
FROM combined