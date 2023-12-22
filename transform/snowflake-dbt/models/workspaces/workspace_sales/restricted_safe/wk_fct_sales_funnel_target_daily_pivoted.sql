WITH final AS (
  
  SELECT  
    target_date, 
    target_date_id,
    report_target_date,
    days_of_month,
    first_day_of_week,
    fiscal_quarter_name,
    target_month_id, 
    first_day_of_month,
    fiscal_year,
    dim_crm_user_hierarchy_sk,
    dim_order_type_id,
    dim_sales_qualified_source_id,

    -- CHURN % ATR
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN daily_allocated_target ELSE 0 END) AS "Churn % ATR Daily",
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN monthly_allocated_target ELSE 0 END) AS "Churn % ATR Monthly",
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN wtd_allocated_target ELSE 0 END) AS "Churn % ATR WTD",
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN mtd_allocated_target ELSE 0 END) AS "Churn % ATR MTD",
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN qtd_allocated_target ELSE 0 END) AS "Churn % ATR QTD",
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN ytd_allocated_target ELSE 0 END) AS "Churn % ATR YTD",

    -- CHURN AMOUNT
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN daily_allocated_target ELSE 0 END) AS "Churn Amount Daily",
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN monthly_allocated_target ELSE 0 END) AS "Churn Amount Monthly",
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN wtd_allocated_target ELSE 0 END) AS "Churn Amount WTD",
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN mtd_allocated_target ELSE 0 END) AS "Churn Amount MTD",
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN qtd_allocated_target ELSE 0 END) AS "Churn Amount QTD",
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN ytd_allocated_target ELSE 0 END) AS "Churn Amount YTD",

    -- CHURN/CONTRACTION AMOUNT
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN daily_allocated_target ELSE 0 END) AS "Churn/Contraction Amount Daily",
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN monthly_allocated_target ELSE 0 END) AS "Churn/Contraction Amount Monthly",
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN wtd_allocated_target ELSE 0 END) AS "Churn/Contraction Amount WTD",
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN mtd_allocated_target ELSE 0 END) AS "Churn/Contraction Amount MTD",
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN qtd_allocated_target ELSE 0 END) AS "Churn/Contraction Amount QTD",
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN ytd_allocated_target ELSE 0 END) AS "Churn/Contraction Amount YTD",

    -- CONTRACTION % ATR
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN daily_allocated_target ELSE 0 END) AS "Contraction % ATR Daily",
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN monthly_allocated_target ELSE 0 END) AS "Contraction % ATR Monthly",
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN wtd_allocated_target ELSE 0 END) AS "Contraction % ATR WTD",
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN mtd_allocated_target ELSE 0 END) AS "Contraction % ATR MTD",
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN qtd_allocated_target ELSE 0 END) AS "Contraction % ATR QTD",
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN ytd_allocated_target ELSE 0 END) AS "Contraction % ATR YTD",

    -- CONTRACTION AMOUNT
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN daily_allocated_target ELSE 0 END) AS "Contraction Amount Daily",
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN monthly_allocated_target ELSE 0 END) AS "Contraction Amount Monthly",
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN wtd_allocated_target ELSE 0 END) AS "Contraction Amount WTD",
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN mtd_allocated_target ELSE 0 END) AS "Contraction Amount MTD",
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN qtd_allocated_target ELSE 0 END) AS "Contraction Amount QTD",
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN ytd_allocated_target ELSE 0 END) AS "Contraction Amount YTD",

    -- DEALS
    MAX(CASE WHEN kpi_name = 'Deals' THEN daily_allocated_target ELSE 0 END) AS "Deals Daily",
    MAX(CASE WHEN kpi_name = 'Deals' THEN monthly_allocated_target ELSE 0 END) AS "Deals Monthly",
    MAX(CASE WHEN kpi_name = 'Deals' THEN wtd_allocated_target ELSE 0 END) AS "Deals WTD",
    MAX(CASE WHEN kpi_name = 'Deals' THEN mtd_allocated_target ELSE 0 END) AS "Deals MTD",
    MAX(CASE WHEN kpi_name = 'Deals' THEN qtd_allocated_target ELSE 0 END) AS "Deals QTD",
    MAX(CASE WHEN kpi_name = 'Deals' THEN ytd_allocated_target ELSE 0 END) AS "Deals YTD",
    
    -- EXPANSION % ATR 
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN daily_allocated_target ELSE 0 END) AS "Expansion % ATR Daily",
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN monthly_allocated_target ELSE 0 END) AS "Expansion % ATR Monthly",
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN wtd_allocated_target ELSE 0 END) AS "Expansion % ATR WTD",
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN mtd_allocated_target ELSE 0 END) AS "Expansion % ATR MTD",
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN qtd_allocated_target ELSE 0 END) AS "Expansion % ATR QTD",
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN ytd_allocated_target ELSE 0 END) AS "Expansion % ATR YTD",

    -- EXPANSION AMOUNT
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN daily_allocated_target ELSE 0 END) AS "Expansion Amount Daily",
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN monthly_allocated_target ELSE 0 END) AS "Expansion Amount Monthly",
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN wtd_allocated_target ELSE 0 END) AS "Expansion Amount WTD",
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN mtd_allocated_target ELSE 0 END) AS "Expansion Amount MTD",
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN qtd_allocated_target ELSE 0 END) AS "Expansion Amount QTD",
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN ytd_allocated_target ELSE 0 END) AS "Expansion Amount YTD",

    -- HIGH LAM STAGE CLOSED WON OPPORTUNITIES
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN daily_allocated_target ELSE 0 END) AS "High LAM Stage Closed Won Opportunities Daily",
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN monthly_allocated_target ELSE 0 END) AS "High LAM Stage Closed Won Opportunities Monthly",
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN wtd_allocated_target ELSE 0 END) AS "High LAM Stage Closed Won Opportunities WTD",
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN mtd_allocated_target ELSE 0 END) AS "High LAM Stage Closed Won Opportunities MTD",
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN qtd_allocated_target ELSE 0 END) AS "High LAM Stage Closed Won Opportunities QTD",
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN ytd_allocated_target ELSE 0 END) AS "High LAM Stage Closed Won Opportunities YTD",

    -- MQL
    MAX(CASE WHEN kpi_name = 'MQL' THEN daily_allocated_target ELSE 0 END) AS "MQL Daily",
    MAX(CASE WHEN kpi_name = 'MQL' THEN monthly_allocated_target ELSE 0 END) AS "MQL Monthly",
    MAX(CASE WHEN kpi_name = 'MQL' THEN wtd_allocated_target ELSE 0 END) AS "MQL WTD",
    MAX(CASE WHEN kpi_name = 'MQL' THEN mtd_allocated_target ELSE 0 END) AS "MQL MTD",
    MAX(CASE WHEN kpi_name = 'MQL' THEN qtd_allocated_target ELSE 0 END) AS "MQL QTD",
    MAX(CASE WHEN kpi_name = 'MQL' THEN ytd_allocated_target ELSE 0 END) AS "MQL YTD",

    -- NET ARR
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN daily_allocated_target ELSE 0 END) AS "Net ARR Daily",
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN monthly_allocated_target ELSE 0 END) AS "Net ARR Monthly",
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN wtd_allocated_target ELSE 0 END) AS "Net ARR WTD",
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN mtd_allocated_target ELSE 0 END) AS "Net ARR MTD",
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN qtd_allocated_target ELSE 0 END) AS "Net ARR QTD",
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN ytd_allocated_target ELSE 0 END) AS "Net ARR YTD",

    --NET ARR COMPANY
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN daily_allocated_target ELSE 0 END) AS "Net ARR Company Daily",
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN monthly_allocated_target ELSE 0 END) AS "Net ARR Company Monthly",
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN wtd_allocated_target ELSE 0 END) AS "Net ARR Company WTD",
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN mtd_allocated_target ELSE 0 END) AS "Net ARR Company MTD",
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN qtd_allocated_target ELSE 0 END) AS "Net ARR Company QTD",
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN ytd_allocated_target ELSE 0 END) AS "Net ARR Company YTD",

    -- NET ARR PIPELINE CREATED
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN daily_allocated_target ELSE 0 END) AS "Net ARR Pipeline Created Daily",
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN monthly_allocated_target ELSE 0 END) AS "Net ARR Pipeline Created Monthly",
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN wtd_allocated_target ELSE 0 END) AS "Net ARR Pipeline Created WTD",
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN mtd_allocated_target ELSE 0 END) AS "Net ARR Pipeline Created MTD",
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN qtd_allocated_target ELSE 0 END) AS "Net ARR Pipeline Created QTD",
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN ytd_allocated_target ELSE 0 END) AS "Net ARR Pipeline Created YTD",

    -- NEW LOGOS
    MAX(CASE WHEN kpi_name = 'New Logos' THEN daily_allocated_target ELSE 0 END) AS "New Logos Daily",
    MAX(CASE WHEN kpi_name = 'New Logos' THEN monthly_allocated_target ELSE 0 END) AS "New Logos Monthly",
    MAX(CASE WHEN kpi_name = 'New Logos' THEN wtd_allocated_target ELSE 0 END) AS "New Logos WTD",
    MAX(CASE WHEN kpi_name = 'New Logos' THEN mtd_allocated_target ELSE 0 END) AS "New Logos MTD",
    MAX(CASE WHEN kpi_name = 'New Logos' THEN qtd_allocated_target ELSE 0 END) AS "New Logos QTD",
    MAX(CASE WHEN kpi_name = 'New Logos' THEN ytd_allocated_target ELSE 0 END) AS "New Logos YTD",
     
    -- PRO SERVE AMOUNT
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN daily_allocated_target ELSE 0 END) AS "ProServe Amount Daily",
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN monthly_allocated_target ELSE 0 END) AS "ProServe Amount Monthly",
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN wtd_allocated_target ELSE 0 END) AS "ProServe Amount",
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN mtd_allocated_target ELSE 0 END) AS "ProServe Amount",
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN qtd_allocated_target ELSE 0 END) AS "ProServe Amount",
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN ytd_allocated_target ELSE 0 END) AS "ProServe Amount",

    -- STAGE 1 OPPORTUNITIES
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN daily_allocated_target ELSE 0 END) AS "Stage 1 Opportunities Daily",
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN monthly_allocated_target ELSE 0 END) AS "Stage 1 Opportunities Monthly",
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN wtd_allocated_target ELSE 0 END) AS "Stage 1 Opportunities WTD",
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN mtd_allocated_target ELSE 0 END) AS "Stage 1 Opportunities MTD",
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN qtd_allocated_target ELSE 0 END) AS "Stage 1 Opportunities QTD",
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN ytd_allocated_target ELSE 0 END) AS "Stage 1 Opportunities YTD",

    -- TOTAL CLOSED
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN daily_allocated_target ELSE 0 END) AS "Total Closed Daily",
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN monthly_allocated_target ELSE 0 END) AS "Total Closed Monthly",
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN wtd_allocated_target ELSE 0 END) AS "Total Closed WTD",
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN mtd_allocated_target ELSE 0 END) AS "Total Closed MTD",
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN qtd_allocated_target ELSE 0 END) AS "Total Closed QTD",
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN ytd_allocated_target ELSE 0 END) AS "Total Closed YTD",
 
    -- TRIALS
    MAX(CASE WHEN kpi_name = 'Trials' THEN daily_allocated_target ELSE 0 END) AS "Trials Daily",
    MAX(CASE WHEN kpi_name = 'Trials' THEN monthly_allocated_target ELSE 0 END) AS "Trials Monthly",
    MAX(CASE WHEN kpi_name = 'Trials' THEN wtd_allocated_target ELSE 0 END) AS "Trials WTD",
    MAX(CASE WHEN kpi_name = 'Trials' THEN mtd_allocated_target ELSE 0 END) AS "Trials MTD",
    MAX(CASE WHEN kpi_name = 'Trials' THEN qtd_allocated_target ELSE 0 END) AS "Trials QTD",
    MAX(CASE WHEN kpi_name = 'Trials' THEN ytd_allocated_target ELSE 0 END) AS "Trials YTD"
FROM {{ref('fct_sales_funnel_target_daily')}}

)

SELECT * 
FROM final