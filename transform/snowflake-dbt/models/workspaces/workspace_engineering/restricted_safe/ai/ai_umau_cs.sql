WITH source AS (

  SELECT
    *,
    code_suggestions_context['data']['gitlab_global_user_id']::VARCHAR AS gitlab_global_user_id,
    code_suggestions_context['data']['gitlab_instance_id']::VARCHAR    AS gitlab_instance_id,
    gitlab_instance_id || '-' || gitlab_global_user_id                 AS instance_user_id
  FROM {{ ref('wk_mart_behavior_structured_event_code_suggestion') }}
  WHERE app_id = 'gitlab_ai_gateway'
    AND gitlab_global_user_id != ''

),

daily_usage AS (

  SELECT
    gitlab_global_user_id,
    gitlab_instance_id,
    instance_user_id,
    behavior_date,
    COUNT(*)                                                                       AS daily_request_count,
    ROW_NUMBER() OVER (PARTITION BY instance_user_id ORDER BY behavior_date)       AS usage_day_n,
    IFF(usage_day_n = 1, TRUE, FALSE)                                              AS is_first_usage_date,
    LAG(behavior_date) OVER (PARTITION BY instance_user_id ORDER BY behavior_date) AS last_usage_date,
    DATEDIFF('day', last_usage_date, behavior_date)                                AS days_since_last_usage
  FROM source
  GROUP BY 1, 2, 3, 4

),

int AS (

  SELECT
    DATE(behavior_at)                       AS day,
    COUNT(DISTINCT source.instance_user_id) AS user_count
  FROM source
  LEFT JOIN daily_usage
    ON source.instance_user_id = daily_usage.instance_user_id
  GROUP BY 1
  ORDER BY 1 DESC
)

SELECT
  day,
  SUM(user_count) OVER (ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS rolling_28_day_umau
FROM int
ORDER BY day DESC
