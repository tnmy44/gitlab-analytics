{% macro date_spine_7th_day() %}

  SELECT
    CASE 
      WHEN date_actual = last_day_of_fiscal_quarter 
        THEN date_actual
      WHEN day_of_fiscal_quarter % 7 = 0 AND day_of_fiscal_quarter != 91
        THEN date_actual
      END AS day_7
  FROM {{ ref('dim_date') }}
  WHERE day_7 IS NOT NULL 

{% endmacro %}