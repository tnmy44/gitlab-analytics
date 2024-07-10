{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH dso_basis AS (

/* Calculate DSO */

  SELECT
    dim_date.fiscal_year,
    dim_date.fiscal_quarter_name_fy                                                                                                                      AS fiscal_quarter,
    rpt_accounting_period_balance_monthly.period,
    rpt_accounting_period_balance_monthly.starting_total_invoice_aging_balance,
    rpt_accounting_period_balance_monthly.total_invoice_aging_balance,
    (rpt_accounting_period_balance_monthly.starting_total_invoice_aging_balance + rpt_accounting_period_balance_monthly.total_invoice_aging_balance) / 2 AS average_ar,
    rpt_accounting_period_balance_monthly.total_billing,
    dim_date.days_in_month_count,
    (average_ar / rpt_accounting_period_balance_monthly.total_billing) * dim_date.days_in_month_count                                                    AS dso
  FROM {{ ref('rpt_accounting_period_balance_monthly') }}
  LEFT JOIN {{ ref('dim_date') }}
    ON rpt_accounting_period_balance_monthly.period = dim_date.date_actual
  ORDER BY rpt_accounting_period_balance_monthly.period

),

dso AS (

/* Round DSO */

  SELECT
    dso_basis.fiscal_year,
    dso_basis.fiscal_quarter,
    dso_basis.period        AS metric_month,
    ROUND(dso_basis.dso, 0) AS dso
  FROM dso_basis

),

cei_basis AS (

/* Calculate CEI */

  SELECT
    dim_date.fiscal_year,
    dim_date.fiscal_quarter_name_fy                                                                                                                                                                                                                                                                                                                                                                      AS fiscal_quarter,
    rpt_accounting_period_balance_monthly.period,
    rpt_accounting_period_balance_monthly.starting_total_invoice_aging_balance,
    rpt_accounting_period_balance_monthly.total_billing,
    rpt_accounting_period_balance_monthly.total_invoice_aging_balance,
    rpt_accounting_period_balance_monthly.ar_current,
    (rpt_accounting_period_balance_monthly.starting_total_invoice_aging_balance + rpt_accounting_period_balance_monthly.total_billing - rpt_accounting_period_balance_monthly.total_invoice_aging_balance) / (rpt_accounting_period_balance_monthly.starting_total_invoice_aging_balance + rpt_accounting_period_balance_monthly.total_billing - rpt_accounting_period_balance_monthly.ar_current) * 100 AS cei
  FROM {{ ref('rpt_accounting_period_balance_monthly') }}
  LEFT JOIN {{ ref('dim_date') }}
    ON rpt_accounting_period_balance_monthly.period = dim_date.date_actual
  ORDER BY rpt_accounting_period_balance_monthly.period

),

cei AS (

/* Round CEI */

  SELECT
    cei_basis.period        AS metric_month,
    ROUND(cei_basis.cei, 2) AS cei
  FROM cei_basis

),

final AS (

  SELECT
    dso.fiscal_year,
    dso.fiscal_quarter,
    dso.metric_month,
    dso.dso,
    cei.cei
  FROM dso
  LEFT JOIN cei
    ON dso.metric_month = cei.metric_month

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-07-09",
updated_date="2024-07-09"
) }}
