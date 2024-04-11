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
  COUNT(DISTINCT b.user_id) AS unique_28d_rolling_count
FROM daily_agg AS a
INNER JOIN
  daily_usage
    AS b
  ON b.behavior_date BETWEEN DATEADD('day', -28, a.day) AND a.day
    AND a.feature = b.event_label
GROUP BY 1, 2
ORDER BY 1 DESC
