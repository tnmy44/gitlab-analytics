{{ simple_cte([
    ('fct_sales_funnel_target_daily', 'fct_sales_funnel_target_daily'),
    ('dim_date', 'dim_date')
    ])

}},


final AS (
  
  SELECT  
    {{ dbt_utils.surrogate_key(['fct_sales_funnel_target_daily.dim_crm_user_hierarchy_sk', 
                                 'dim_date.fiscal_year', 
                                 'dim_date.first_day_of_month', 
                                 'fct_sales_funnel_target_daily.dim_sales_qualified_source_id',
                                 'fct_sales_funnel_target_daily.dim_order_type_id',
                                 'dim_date.date_day'
                                 ]) }}                                                                                  AS actuals_targets_daily_pk,
    fct_sales_funnel_target_daily.target_date, 
    fct_sales_funnel_target_daily.target_date_id,
    fct_sales_funnel_target_daily.report_target_date,
    fct_sales_funnel_target_daily.dim_crm_user_hierarchy_sk,
    fct_sales_funnel_target_daily.dim_order_type_id,
    fct_sales_funnel_target_daily.dim_sales_qualified_source_id,
    fct_sales_funnel_target_daily.days_of_month,
    fct_sales_funnel_target_daily.first_day_of_week,
    fct_sales_funnel_target_daily.fiscal_quarter_name,
    fct_sales_funnel_target_daily.target_month_id, 
    fct_sales_funnel_target_daily.first_day_of_month,
    fct_sales_funnel_target_daily.fiscal_year,
    dim_date.day_of_week,
    dim_date.date_id,
    dim_date.fiscal_month_name_fy,
    dim_date.fiscal_quarter_name_fy,
    dim_date.first_day_of_fiscal_quarter,
    dim_date.first_day_of_fiscal_year,
    dim_date.last_day_of_week,
    dim_date.last_day_of_month,
    dim_date.last_day_of_fiscal_quarter,
    dim_date.last_day_of_fiscal_year,
    -- CHURN % ATR
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN daily_allocated_target ELSE 0 END) AS "Churn % ATR Daily Target",
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN monthly_allocated_target ELSE 0 END) AS "Churn % ATR Monthly Target",
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN wtd_allocated_target ELSE 0 END) AS "Churn % ATR WTD Target",
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN mtd_allocated_target ELSE 0 END) AS "Churn % ATR MTD Target",
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN qtd_allocated_target ELSE 0 END) AS "Churn % ATR QTD Target",
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN ytd_allocated_target ELSE 0 END) AS "Churn % ATR YTD Target",

    -- CHURN AMOUNT
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN daily_allocated_target ELSE 0 END) AS "Churn Amount Daily Target",
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN monthly_allocated_target ELSE 0 END) AS "Churn Amount Monthly Target",
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN wtd_allocated_target ELSE 0 END) AS "Churn Amount WTD Target",
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN mtd_allocated_target ELSE 0 END) AS "Churn Amount MTD Target",
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN qtd_allocated_target ELSE 0 END) AS "Churn Amount QTD Target",
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN ytd_allocated_target ELSE 0 END) AS "Churn Amount YTD Target",

    -- CHURN/CONTRACTION AMOUNT
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN daily_allocated_target ELSE 0 END) AS "Churn/Contraction Amount Daily Target",
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN monthly_allocated_target ELSE 0 END) AS "Churn/Contraction Amount Monthly Target",
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN wtd_allocated_target ELSE 0 END) AS "Churn/Contraction Amount WTD Target",
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN mtd_allocated_target ELSE 0 END) AS "Churn/Contraction Amount MTD Target",
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN qtd_allocated_target ELSE 0 END) AS "Churn/Contraction Amount QTD Target",
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN ytd_allocated_target ELSE 0 END) AS "Churn/Contraction Amount YTD Target",

    -- CONTRACTION % ATR
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN daily_allocated_target ELSE 0 END) AS "Contraction % ATR Daily Target",
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN monthly_allocated_target ELSE 0 END) AS "Contraction % ATR Monthly Target",
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN wtd_allocated_target ELSE 0 END) AS "Contraction % ATR WTD Target",
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN mtd_allocated_target ELSE 0 END) AS "Contraction % ATR MTD Target",
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN qtd_allocated_target ELSE 0 END) AS "Contraction % ATR QTD Target",
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN ytd_allocated_target ELSE 0 END) AS "Contraction % ATR YTD Target",

    -- CONTRACTION AMOUNT
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN daily_allocated_target ELSE 0 END) AS "Contraction Amount Daily Target",
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN monthly_allocated_target ELSE 0 END) AS "Contraction Amount Monthly Target",
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN wtd_allocated_target ELSE 0 END) AS "Contraction Amount WTD Target",
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN mtd_allocated_target ELSE 0 END) AS "Contraction Amount MTD Target",
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN qtd_allocated_target ELSE 0 END) AS "Contraction Amount QTD Target",
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN ytd_allocated_target ELSE 0 END) AS "Contraction Amount YTD Target",

    -- DEALS
    MAX(CASE WHEN kpi_name = 'Deals' THEN daily_allocated_target ELSE 0 END) AS "Deals Daily Target",
    MAX(CASE WHEN kpi_name = 'Deals' THEN monthly_allocated_target ELSE 0 END) AS "Deals Monthly Target",
    MAX(CASE WHEN kpi_name = 'Deals' THEN wtd_allocated_target ELSE 0 END) AS "Deals WTD Target",
    MAX(CASE WHEN kpi_name = 'Deals' THEN mtd_allocated_target ELSE 0 END) AS "Deals MTD Target",
    MAX(CASE WHEN kpi_name = 'Deals' THEN qtd_allocated_target ELSE 0 END) AS "Deals QTD Target",
    MAX(CASE WHEN kpi_name = 'Deals' THEN ytd_allocated_target ELSE 0 END) AS "Deals YTD Target",
    
    -- EXPANSION % ATR 
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN daily_allocated_target ELSE 0 END) AS "Expansion % ATR Daily Target",
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN monthly_allocated_target ELSE 0 END) AS "Expansion % ATR Monthly Target",
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN wtd_allocated_target ELSE 0 END) AS "Expansion % ATR WTD Target",
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN mtd_allocated_target ELSE 0 END) AS "Expansion % ATR MTD Target",
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN qtd_allocated_target ELSE 0 END) AS "Expansion % ATR QTD Target",
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN ytd_allocated_target ELSE 0 END) AS "Expansion % ATR YTD Target",

    -- EXPANSION AMOUNT
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN daily_allocated_target ELSE 0 END) AS "Expansion Amount Daily Target",
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN monthly_allocated_target ELSE 0 END) AS "Expansion Amount Monthly Target",
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN wtd_allocated_target ELSE 0 END) AS "Expansion Amount WTD Target",
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN mtd_allocated_target ELSE 0 END) AS "Expansion Amount MTD Target",
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN qtd_allocated_target ELSE 0 END) AS "Expansion Amount QTD Target",
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN ytd_allocated_target ELSE 0 END) AS "Expansion Amount YTD Target",

    -- HIGH LAM STAGE CLOSED WON OPPORTUNITIES
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN daily_allocated_target ELSE 0 END) AS "High LAM Stage Closed Won Opportunities Daily Target",
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN monthly_allocated_target ELSE 0 END) AS "High LAM Stage Closed Won Opportunities Monthly Target",
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN wtd_allocated_target ELSE 0 END) AS "High LAM Stage Closed Won Opportunities WTD Target",
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN mtd_allocated_target ELSE 0 END) AS "High LAM Stage Closed Won Opportunities MTD Target",
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN qtd_allocated_target ELSE 0 END) AS "High LAM Stage Closed Won Opportunities QTD Target",
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN ytd_allocated_target ELSE 0 END) AS "High LAM Stage Closed Won Opportunities YTD Target",

    -- MQL
    MAX(CASE WHEN kpi_name = 'MQL' THEN daily_allocated_target ELSE 0 END) AS "MQL Daily Target",
    MAX(CASE WHEN kpi_name = 'MQL' THEN monthly_allocated_target ELSE 0 END) AS "MQL Monthly Target",
    MAX(CASE WHEN kpi_name = 'MQL' THEN wtd_allocated_target ELSE 0 END) AS "MQL WTD Target",
    MAX(CASE WHEN kpi_name = 'MQL' THEN mtd_allocated_target ELSE 0 END) AS "MQL MTD Target",
    MAX(CASE WHEN kpi_name = 'MQL' THEN qtd_allocated_target ELSE 0 END) AS "MQL QTD Target",
    MAX(CASE WHEN kpi_name = 'MQL' THEN ytd_allocated_target ELSE 0 END) AS "MQL YTD Target",

    -- NET ARR
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN daily_allocated_target ELSE 0 END) AS "Net ARR Daily Target",
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN monthly_allocated_target ELSE 0 END) AS "Net ARR Monthly Target",
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN wtd_allocated_target ELSE 0 END) AS "Net ARR WTD Target",
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN mtd_allocated_target ELSE 0 END) AS "Net ARR MTD Target",
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN qtd_allocated_target ELSE 0 END) AS "Net ARR QTD Target",
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN ytd_allocated_target ELSE 0 END) AS "Net ARR YTD Target",

    --NET ARR COMPANY
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN daily_allocated_target ELSE 0 END) AS "Net ARR Company Daily Target",
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN monthly_allocated_target ELSE 0 END) AS "Net ARR Company Monthly Target",
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN wtd_allocated_target ELSE 0 END) AS "Net ARR Company WTD Target",
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN mtd_allocated_target ELSE 0 END) AS "Net ARR Company MTD Target",
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN qtd_allocated_target ELSE 0 END) AS "Net ARR Company QTD Target",
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN ytd_allocated_target ELSE 0 END) AS "Net ARR Company YTD Target",

    -- NET ARR PIPELINE CREATED
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN daily_allocated_target ELSE 0 END) AS "Net ARR Pipeline Created Daily Target",
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN monthly_allocated_target ELSE 0 END) AS "Net ARR Pipeline Created Monthly Target",
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN wtd_allocated_target ELSE 0 END) AS "Net ARR Pipeline Created WTD Target",
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN mtd_allocated_target ELSE 0 END) AS "Net ARR Pipeline Created MTD Target",
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN qtd_allocated_target ELSE 0 END) AS "Net ARR Pipeline Created QTD Target",
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN ytd_allocated_target ELSE 0 END) AS "Net ARR Pipeline Created YTD Target",

    -- NEW LOGOS
    MAX(CASE WHEN kpi_name = 'New Logos' THEN daily_allocated_target ELSE 0 END) AS "New Logos Daily Target",
    MAX(CASE WHEN kpi_name = 'New Logos' THEN monthly_allocated_target ELSE 0 END) AS "New Logos Monthly Target",
    MAX(CASE WHEN kpi_name = 'New Logos' THEN wtd_allocated_target ELSE 0 END) AS "New Logos WTD Target",
    MAX(CASE WHEN kpi_name = 'New Logos' THEN mtd_allocated_target ELSE 0 END) AS "New Logos MTD Target",
    MAX(CASE WHEN kpi_name = 'New Logos' THEN qtd_allocated_target ELSE 0 END) AS "New Logos QTD Target",
    MAX(CASE WHEN kpi_name = 'New Logos' THEN ytd_allocated_target ELSE 0 END) AS "New Logos YTD Target",
     
    -- PRO SERVE AMOUNT
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN daily_allocated_target ELSE 0 END) AS "ProServe Amount Daily Target",
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN monthly_allocated_target ELSE 0 END) AS "ProServe Amount Monthly Target",
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN wtd_allocated_target ELSE 0 END) AS "ProServe Amount WTD Target",
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN mtd_allocated_target ELSE 0 END) AS "ProServe Amount MTD Target",
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN qtd_allocated_target ELSE 0 END) AS "ProServe Amount QTD Target",
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN ytd_allocated_target ELSE 0 END) AS "ProServe Amount YTD Target",

    -- STAGE 1 OPPORTUNITIES
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN daily_allocated_target ELSE 0 END) AS "Stage 1 Opportunities Daily Target",
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN monthly_allocated_target ELSE 0 END) AS "Stage 1 Opportunities Monthly Target",
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN wtd_allocated_target ELSE 0 END) AS "Stage 1 Opportunities WTD Target",
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN mtd_allocated_target ELSE 0 END) AS "Stage 1 Opportunities MTD Target",
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN qtd_allocated_target ELSE 0 END) AS "Stage 1 Opportunities QTD Target",
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN ytd_allocated_target ELSE 0 END) AS "Stage 1 Opportunities YTD Target",

    -- TOTAL CLOSED
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN daily_allocated_target ELSE 0 END) AS "Total Closed Daily Target",
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN monthly_allocated_target ELSE 0 END) AS "Total Closed Monthly Target",
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN wtd_allocated_target ELSE 0 END) AS "Total Closed WTD Target",
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN mtd_allocated_target ELSE 0 END) AS "Total Closed MTD Target",
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN qtd_allocated_target ELSE 0 END) AS "Total Closed QTD Target",
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN ytd_allocated_target ELSE 0 END) AS "Total Closed YTD Target",
 
    -- TRIALS
    MAX(CASE WHEN kpi_name = 'Trials' THEN daily_allocated_target ELSE 0 END) AS "Trials Daily Target",
    MAX(CASE WHEN kpi_name = 'Trials' THEN monthly_allocated_target ELSE 0 END) AS "Trials Monthly Target",
    MAX(CASE WHEN kpi_name = 'Trials' THEN wtd_allocated_target ELSE 0 END) AS "Trials WTD Target",
    MAX(CASE WHEN kpi_name = 'Trials' THEN mtd_allocated_target ELSE 0 END) AS "Trials MTD Target",
    MAX(CASE WHEN kpi_name = 'Trials' THEN qtd_allocated_target ELSE 0 END) AS "Trials QTD Target",
    MAX(CASE WHEN kpi_name = 'Trials' THEN ytd_allocated_target ELSE 0 END) AS "Trials YTD Target"
FROM fct_sales_funnel_target_daily
LEFT JOIN dim_date
  ON fct_sales_funnel_target_daily.target_date = dim_date.date_actual
{{ dbt_utils.group_by(n=23)}} 

)

SELECT * 
FROM final