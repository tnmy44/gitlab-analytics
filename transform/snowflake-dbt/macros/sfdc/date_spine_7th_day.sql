{% macro date_spine_7th_day() %}

  SELECT
    CASE 
      WHEN dim_date.date_actual = dim_date.last_day_of_fiscal_quarter 
        THEN dim_date.date_actual
      WHEN dim_date.day_of_fiscal_quarter % 7 = 0 AND dim_date.day_of_fiscal_quarter != 91
        THEN dim_date.date_actual
      END AS date_actual,
      dim_date.date_id,
      dim_date.first_day_of_fiscal_quarter,
      dim_date.last_day_of_fiscal_quarter,
      dim_date.day_of_fiscal_quarter,
      dim_date.fiscal_quarter_name_fy
  FROM {{ ref('dim_date') }}
  WHERE date_actual IS NOT NULL 

{% endmacro %}