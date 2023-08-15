WITH source AS (

  SELECT *
  FROM {{ ref('date_details_source') }}

), current_dates AS (

  SELECT
    day_of_month          AS curernt_day_of_month,
    day_of_fiscal_quarter AS curernt_day_of_fiscal_quarter,
    day_of_fiscal_year    AS current_day_of_fiscal_year
  FROM source
  WHERE date_actual = CURRENT_DATE

)

, final AS (

  SELECT
    *,
    IFF(day_of_month <= current_dates.curernt_day_of_month, TRUE, FALSE)                                          AS is_fiscal_month_to_date,
    IFF(day_of_fiscal_quarter <= current_dates.curernt_day_of_fiscal_quarter, TRUE, FALSE)                        AS is_fiscal_quarter_to_date,
    IFF(day_of_fiscal_year <= current_dates.current_day_of_fiscal_year, TRUE, FALSE)                              AS is_fiscal_year_to_date,
    DATEDIFF('days', date_actual, CURRENT_DATE)                                                                   AS fiscal_days_ago,
    DATEDIFF('week', date_actual, CURRENT_DATE)                                                                   AS fiscal_weeks_ago,
    DATEDIFF('months', first_day_of_month, current_first_day_of_month)                                            AS fiscal_months_ago,
    ROUND(DATEDIFF('months', first_day_of_fiscal_quarter, current_first_day_of_fiscal_quarter) / 3, 0)            AS fiscal_quarters_ago,
    ROUND(DATEDIFF('months', first_day_of_fiscal_year, current_first_day_of_fiscal_year) / 12, 0)                 AS fiscal_years_ago
 
  FROM source
  LEFT JOIN current_dates -- Can join without an ON clause since this CTE only has one row

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@pempey",
    updated_by="@jpeguero",
    created_date="2023-01-09",
    updated_date="2023-08-15"
) }}
