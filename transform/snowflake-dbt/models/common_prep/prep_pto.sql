WITH pto AS (
  SELECT *
  FROM {{ ref('gitlab_pto') }}
),
prep_date AS (
  SELECT *
  FROM {{ ref('prep_date') }}
),
final AS (
  SELECT
    pto.*,
    prep_date.day_of_week AS pto_day_of_week,
    'Y'                   AS is_pto_date,
    IFF(
      pto.pto_type_name = 'Out Sick'
      AND DATEDIFF('day', pto.start_date, pto.end_date) > 4, 'Out Sick-Extended', pto.pto_type_name
    )                     AS absence_status
  FROM pto
  LEFT JOIN prep_date
    ON pto.pto_date = prep_date.date_actual
  WHERE pto.pto_status = 'AP'
    AND pto_day_of_week BETWEEN 2 AND 6
)

SELECT *
FROM final
