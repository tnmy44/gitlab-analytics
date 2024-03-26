WITH dau AS (
  SELECT
    e.behavior_date                             AS mmyyyy,
    COUNT(DISTINCT e.gsc_pseudonymized_user_id) AS dau
  FROM {{ ref('mart_behavior_structured_event') }} AS e
  WHERE
    behavior_at BETWEEN '2023-04-21' AND CURRENT_DATE --first date of these events
    AND
    (
      e.event_label = 'chat'
      OR
      e.event_label = 'gitlab_duo_chat_answer'
    )
  GROUP BY ALL
  ORDER BY
    1 DESC
),

dim_date AS (

  SELECT * FROM {{ ref('dim_date') }}
  WHERE
    date_day BETWEEN '2023-04-21' AND CURRENT_DATE
),

aus AS (
  SELECT
    date_day AS day,
    dau.dau
  FROM
    dim_date
  LEFT JOIN
    dau ON dim_date.date_day = dau.mmyyyy
)

SELECT
  day,
  SUM(dau) OVER (ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS rolling_28_day_umau
FROM aus
ORDER BY 1 DESC
