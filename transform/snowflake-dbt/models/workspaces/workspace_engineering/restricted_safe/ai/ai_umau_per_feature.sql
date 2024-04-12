WITH daily_usage AS (
  SELECT
    event_label,
    e.behavior_date             AS behavior_date,
    e.gsc_pseudonymized_user_id AS user_id
  FROM {{ ref('mart_behavior_structured_event') }} AS e
  WHERE
    behavior_at BETWEEN '2023-04-21' AND CURRENT_DATE --first date of these events
    AND
    (
      e.event_action = 'execute_llm_method'
    )
  GROUP BY ALL
  ORDER BY
    1 DESC
),

cs AS (

  SELECT
    cs.day,
    'code_suggestions' AS feature,
    cs.dau,
    cs.unique_7d_rolling_count,
    cs.unique_28d_rolling_count
  FROM {{ ref('ai_umau_cs') }} AS cs

),

daily_agg AS (
  SELECT
    behavior_date           AS day,
    event_label             AS feature,
    COUNT(DISTINCT user_id) AS user_count
  FROM daily_usage
  GROUP BY 1, 2
)

SELECT
  a.day,
  a.feature,
  COUNT(DISTINCT b.user_id) AS unique_daily_rolling_count,
  COUNT(DISTINCT c.user_id) AS unique_7d_rolling_count,
  COUNT(DISTINCT b.user_id) AS unique_28d_rolling_count
FROM daily_agg AS a
INNER JOIN
  daily_usage
    AS b
  ON b.behavior_date BETWEEN DATEADD('day', -28, a.day) AND a.day
    AND a.feature = b.event_label
INNER JOIN
  daily_usage
    AS c
  ON c.behavior_date BETWEEN DATEADD('day', -7, a.day) AND a.day
    AND a.feature = c.event_label
GROUP BY 1, 2
UNION ALL
SELECT * FROM cs
