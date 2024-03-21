{% macro date_spine_7th_day(date_model) %}

  SELECT
    CASE 
      WHEN date_actual = last_day_of_fiscal_quarter 
        THEN date_actual
      WHEN day_of_fiscal_quarter % 7 = 0 AND day_of_fiscal_quarter != 91
        THEN date_actual
      END AS day_7_current_week,
      day_of_fiscal_quarter,
      LAG(day_7_current_week) OVER (ORDER BY day_7_current_week) + 1 AS day_8_previous_week
  FROM {{ ref(date_model) }}
  WHERE day_7_current_week IS NOT NULL 
    AND date_actual >= DATEADD(YEAR, -2, current_first_day_of_fiscal_quarter) -- include only the last 8 quarters 

{% endmacro %}