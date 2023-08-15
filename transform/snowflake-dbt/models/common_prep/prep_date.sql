WITH source AS (
  SELECT *
  FROM {{ ref('date_details_source') }}
), final AS (

  SELECT
    *,
    IFF(first_day_of_month = current_first_day_of_month AND CURRENT_DATE >= date_actual, TRUE, FALSE)             AS is_fiscal_month_to_date,
    IFF(fiscal_quarter_name_fy = current_fiscal_quarter_name_fy AND CURRENT_DATE >= date_actual, TRUE, FALSE)     AS is_fiscal_quarter_to_date,
    IFF(first_day_of_fiscal_year = current_first_day_of_fiscal_year AND CURRENT_DATE >= date_actual, TRUE, FALSE) AS is_fiscal_year_to_date,
    DATEDIFF('days', date_actual, CURRENT_DATE)                                                                   AS fiscal_days_ago,
    DATEDIFF('week', date_actual, CURRENT_DATE)                                                                   AS fiscal_weeks_ago,
    DATEDIFF('months', first_day_of_month, current_first_day_of_month)                                            AS fiscal_months_ago,
    ROUND(DATEDIFF('months', first_day_of_fiscal_quarter, current_first_day_of_fiscal_quarter) / 3, 0)            AS fiscal_quarters_ago,
    ROUND(DATEDIFF('months', first_day_of_fiscal_year, current_first_day_of_fiscal_year) / 12, 0)                 AS fiscal_years_ago
 
  FROM source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@pempey",
    updated_by="@jpeguero",
    created_date="2023-01-09",
    updated_date="2023-08-15"
) }}
