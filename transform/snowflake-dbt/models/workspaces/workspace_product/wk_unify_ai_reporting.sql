{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('dim_date', 'dim_date')
]) }},

prep AS (

  SELECT
    event_label,
    plan_name_modified                                  AS plan_name,
    CASE
      WHEN gsc_is_gitlab_team_member IN ('false', 'e08c592bd39b012f7c83bbc0247311b238ee1caa61be28ccfd412497290f896a') 
        THEN 'External'
      WHEN gsc_is_gitlab_team_member IN ('true', '5616b37fa230003bc8510af409bf3f5970e6d5027cc282b0ab3080700d92e7ad') 
        THEN 'Internal'
      ELSE 'Unknown'
    END                                                 AS internal_or_external,
    behavior_date,
    gsc_pseudonymized_user_id,
    DATE_TRUNC(WEEK, behavior_date)                     AS current_week,
    DATE_TRUNC(WEEK, DATEADD(DAY, 7, behavior_date))    AS next_week,
    DATE_TRUNC(MONTH, behavior_date)                    AS current_month,
    DATE_TRUNC(MONTH, DATEADD(MONTH, 1, behavior_date)) AS next_month
  FROM {{ ref('mart_behavior_structured_event') }}
  WHERE event_action = 'execute_llm_method'
    AND behavior_date BETWEEN '2023-04-21' AND CURRENT_DATE
    AND event_category = 'Llm::ExecuteMethodService'
    AND event_label != 'code_suggestions'

), 

metric_prep AS (
  
  SELECT *
  FROM
    {{ ref('mart_ping_instance_metric') }}
  WHERE major_minor_version_id >= 1611
    AND metric_value > 0
    AND is_last_ping_of_month = TRUE
    AND ping_created_date_month > '2024-01-01'
    AND ping_deployment_type != 'GitLab.com'

),

week_all AS (

  SELECT
    DATE_TRUNC(WEEK, behavior_date)           AS _date,
    event_label,
    'All'                                     AS plan,
    'All'                                     AS internal_or_external,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS user_count
  FROM prep
  GROUP BY ALL

),

week_add_plan AS (

  SELECT
    DATE_TRUNC(WEEK, behavior_date)           AS _date,
    event_label,
    plan_name,
    'All',
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS user_count
  FROM prep
  GROUP BY ALL

),

week_split AS (

  SELECT
    DATE_TRUNC(WEEK, behavior_date)           AS _date,
    event_label,
    plan_name,
    internal_or_external,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS user_count
  FROM prep
  GROUP BY ALL

),

week_int_ext AS (

  SELECT
    DATE_TRUNC(WEEK, behavior_date)           AS _date,
    event_label,
    'All',
    internal_or_external,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS user_count
  FROM prep
  GROUP BY ALL

),

wau AS (

  SELECT *
  FROM week_all
  WHERE _date < DATE_TRUNC(WEEK, CURRENT_DATE())

  UNION ALL

  SELECT *
  FROM week_add_plan
  WHERE _date < DATE_TRUNC(WEEK, CURRENT_DATE())

  UNION ALL

  SELECT *
  FROM week_split
  WHERE _date < DATE_TRUNC(WEEK, CURRENT_DATE())

  UNION ALL

  SELECT *
  FROM week_int_ext
  WHERE _date < DATE_TRUNC(WEEK, CURRENT_DATE())

),

day_all AS (

  SELECT
    DATE_TRUNC(DAY, behavior_date)            AS _date,
    event_label,
    'All'                                     AS plan,
    'All'                                     AS internal_or_external,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS metric_value
  FROM prep
  GROUP BY ALL

),

day_add_plan AS (

  SELECT
    DATE_TRUNC(DAY, behavior_date)            AS _date,
    event_label,
    plan_name,
    'All',
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS user_count
  FROM prep
  GROUP BY ALL

),

day_split AS (

  SELECT
    DATE_TRUNC(DAY, behavior_date)            AS _date,
    event_label,
    plan_name,
    internal_or_external,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS user_count
  FROM prep
  GROUP BY ALL

),

day_int_ext AS (
  
  SELECT
    DATE_TRUNC(DAY, behavior_date)            AS _date,
    event_label,
    'All',
    internal_or_external,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS user_count
  FROM prep
  GROUP BY ALL

),

dau AS (
  
  SELECT *
  FROM day_all
  
  UNION ALL
  
  SELECT *
  FROM day_add_plan

  UNION ALL
  
  SELECT *
  FROM day_split
  
  UNION ALL
  
  SELECT *
  FROM day_int_ext

),

month_all AS (
  
  SELECT
    DATE_TRUNC(MONTH, behavior_date)          AS _date,
    event_label,
    'All'                                     AS plan,
    'All'                                     AS internal_or_external,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS user_count
  FROM prep
  GROUP BY ALL

),

month_add_plan AS (
  
  SELECT
    DATE_TRUNC(MONTH, behavior_date)          AS _date,
    event_label,
    plan_name,
    'All',
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS user_count
  FROM prep
  GROUP BY ALL

),

month_split AS (
  
  SELECT
    DATE_TRUNC(MONTH, behavior_date)          AS _date,
    event_label,
    plan_name,
    internal_or_external,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS user_count
  FROM prep
  GROUP BY ALL

),

month_int_ext AS (

  SELECT
    DATE_TRUNC(MONTH, behavior_date)          AS _date,
    event_label,
    'All',
    internal_or_external,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS user_count
  FROM
    prep
  GROUP BY ALL

),

mau AS (
  
  SELECT *
  FROM month_all
  WHERE _date < DATE_TRUNC(MONTH, CURRENT_DATE())
  
  UNION ALL
  
  SELECT *
  FROM month_add_plan
  WHERE _date < DATE_TRUNC(MONTH, CURRENT_DATE())
  
  UNION ALL
  
  SELECT *
  FROM month_split
  WHERE _date < DATE_TRUNC(MONTH, CURRENT_DATE())
  
  UNION ALL
  
  SELECT *
  FROM month_int_ext
  WHERE _date < DATE_TRUNC(MONTH, CURRENT_DATE())

),

weekly_retention_grouped AS (
  SELECT
    prep.current_week,
    prep.event_label,
    prep.plan_name,
    prep.internal_or_external,
    (COUNT(DISTINCT e2.gsc_pseudonymized_user_id) / COUNT(DISTINCT prep.gsc_pseudonymized_user_id)) AS retention_rate
  FROM prep
  LEFT JOIN prep AS e2
    ON e2.event_label = prep.event_label 
      AND e2.gsc_pseudonymized_user_id = prep.gsc_pseudonymized_user_id 
      AND e2.current_week = prep.next_week
  WHERE prep.next_week < DATE_TRUNC(WEEK, CURRENT_DATE())
  GROUP BY ALL
  HAVING COUNT(DISTINCT prep.gsc_pseudonymized_user_id) > 0

),

monthly_retention_grouped AS (
  SELECT
    prep.current_month,
    prep.event_label,
    prep.plan_name,
    prep.internal_or_external,
    (COUNT(DISTINCT e2.gsc_pseudonymized_user_id) / COUNT(DISTINCT prep.gsc_pseudonymized_user_id)) AS retention_rate
  FROM prep
  LEFT JOIN prep AS e2
    ON e2.event_label = prep.event_label 
      AND e2.gsc_pseudonymized_user_id = prep.gsc_pseudonymized_user_id 
      AND e2.current_month = prep.next_month
  WHERE prep.next_month < DATE_TRUNC(MONTH, CURRENT_DATE())
  GROUP BY ALL
  HAVING COUNT(DISTINCT prep.gsc_pseudonymized_user_id) > 0

),

weekly_retention_no_plan AS (
  
  SELECT
    prep.current_week,
    prep.event_label,
    'All',
    prep.internal_or_external,
    (COUNT(DISTINCT e2.gsc_pseudonymized_user_id)/COUNT(DISTINCT prep.gsc_pseudonymized_user_id)) AS retention_rate
  FROM prep
  LEFT JOIN prep AS e2 
    ON e2.event_label = prep.event_label 
      AND e2.gsc_pseudonymized_user_id = prep.gsc_pseudonymized_user_id 
      AND e2.current_week = prep.next_week
  WHERE prep.next_week < DATE_TRUNC(WEEK,CURRENT_DATE())
  GROUP BY ALL
  HAVING COUNT(DISTINCT prep.gsc_pseudonymized_user_id) > 0

),

monthly_retention_no_plan AS (

  SELECT
    prep.current_month,
    prep.event_label,
    'All',
    prep.internal_or_external,
    (COUNT(DISTINCT e2.gsc_pseudonymized_user_id) / COUNT(DISTINCT prep.gsc_pseudonymized_user_id)) AS retention_rate
  FROM prep
  LEFT JOIN prep AS e2
    ON e2.event_label = prep.event_label 
      AND e2.gsc_pseudonymized_user_id = prep.gsc_pseudonymized_user_id 
      AND e2.current_month = prep.next_month
  WHERE prep.next_month < DATE_TRUNC(MONTH, CURRENT_DATE())
  GROUP BY ALL
  HAVING COUNT(DISTINCT prep.gsc_pseudonymized_user_id) > 0

),

weekly_retention_no_intext AS (

  SELECT
    prep.current_week,
    prep.event_label,
    prep.plan_name,
    'All',
    (COUNT(DISTINCT e2.gsc_pseudonymized_user_id) / COUNT(DISTINCT prep.gsc_pseudonymized_user_id)) AS retention_rate
  FROM prep
  LEFT JOIN prep AS e2
    ON e2.event_label = prep.event_label 
      AND e2.gsc_pseudonymized_user_id = prep.gsc_pseudonymized_user_id 
      AND e2.current_week = prep.next_week
  WHERE prep.next_week < DATE_TRUNC(WEEK, CURRENT_DATE())
  GROUP BY ALL
  HAVING COUNT(DISTINCT prep.gsc_pseudonymized_user_id) > 0

),

monthly_retention_no_intext AS (

  SELECT
    prep.current_month,
    prep.event_label,
    prep.plan_name,
    'All',
    (COUNT(DISTINCT e2.gsc_pseudonymized_user_id) / COUNT(DISTINCT prep.gsc_pseudonymized_user_id)) AS retention_rate
  FROM prep
  LEFT JOIN prep AS e2
    ON e2.event_label = prep.event_label 
      AND e2.gsc_pseudonymized_user_id = prep.gsc_pseudonymized_user_id 
      AND e2.current_month = prep.next_month
  WHERE prep.next_month < DATE_TRUNC(MONTH, CURRENT_DATE())
  GROUP BY ALL
  HAVING COUNT(DISTINCT prep.gsc_pseudonymized_user_id) > 0

),

weekly_retention_all AS (
  SELECT
    prep.current_week,
    prep.event_label,
    'All',
    'All',
    (COUNT(DISTINCT e2.gsc_pseudonymized_user_id) / COUNT(DISTINCT prep.gsc_pseudonymized_user_id)) AS retention_rate
  FROM prep
  LEFT JOIN prep AS e2
    ON e2.event_label = prep.event_label 
      AND e2.gsc_pseudonymized_user_id = prep.gsc_pseudonymized_user_id 
      AND e2.current_week = prep.next_week
  WHERE prep.next_week < DATE_TRUNC(WEEK, CURRENT_DATE())
  GROUP BY ALL
  HAVING COUNT(DISTINCT prep.gsc_pseudonymized_user_id) > 0

),

monthly_retention_all AS (

  SELECT
    prep.current_month,
    prep.event_label,
    'All',
    'All',
    (COUNT(DISTINCT e2.gsc_pseudonymized_user_id) / COUNT(DISTINCT prep.gsc_pseudonymized_user_id)) AS retention_rate
  FROM prep
  LEFT JOIN prep AS e2
    ON e2.event_label = prep.event_label 
      AND e2.gsc_pseudonymized_user_id = prep.gsc_pseudonymized_user_id 
      AND e2.current_month = prep.next_month
  WHERE prep.next_month < DATE_TRUNC(MONTH, CURRENT_DATE())
  GROUP BY ALL
  HAVING COUNT(DISTINCT prep.gsc_pseudonymized_user_id) > 0
),

retentions AS (
  SELECT
    *,
    'Weekly Retention' AS metric
  FROM weekly_retention_grouped
  
  UNION ALL
  
  SELECT
    *,
    'Monthly Retention' AS metric
  FROM monthly_retention_grouped
  
  UNION ALL
  
  SELECT
    *,
    'Weekly Retention' AS metric
  FROM weekly_retention_no_plan
  
  UNION ALL
  
  SELECT
    *,
    'Monthly Retention' AS metric
  FROM monthly_retention_no_plan
  
  UNION ALL
  
  SELECT
    *,
    'Weekly Retention' AS metric
  FROM weekly_retention_no_intext
  
  UNION ALL
  
  SELECT
    *,
    'Monthly Retention' AS metric
  FROM monthly_retention_no_intext
  
  UNION ALL
  
  SELECT
    *,
    'Weekly Retention' AS metric
  FROM weekly_retention_all
  
  UNION ALL
  
  SELECT
    *,
    'Monthly Retention' AS metric
  FROM monthly_retention_all


),

metrics AS (
  
  SELECT
    *,
    'DAU' AS metric
  FROM dau
  
  UNION ALL

  SELECT
    *,
    'WAU' AS metric
  FROM wau
  WHERE _date < DATE_TRUNC(WEEK, CURRENT_DATE)
  
  UNION ALL

  SELECT
    *,
    'MAU' AS metric
  FROM mau
  WHERE _date < DATE_TRUNC(MONTH, CURRENT_DATE)
  
  UNION ALL

  SELECT *
  FROM retentions

),

unify AS (

  SELECT
    dim_date.date_day::DATE       AS date_day,
    metrics.event_label           AS ai_feature,
    metrics.plan,
    metrics.internal_or_external,
    'Gitlab.com'                  AS delivery_type,
    metrics.metric_value,
    metrics.metric
  FROM dim_date
  LEFT JOIN metrics 
    ON dim_date.date_day = metrics._date
  WHERE dim_date.date_day BETWEEN '2023-04-21' AND CURRENT_DATE

  UNION ALL

  SELECT
    ping_created_date_month::DATE       AS date_day,
    'chat'                              AS ai_feature,
    LOWER(ping_product_tier)            AS plan,
    'External'                          AS internal_or_external,
    ping_deployment_type                AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'MAU'                               AS metric
  FROM metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_monthly'
  GROUP BY ALL

  UNION ALL

  SELECT
    DATE_TRUNC(WEEK, ping_created_date_week)::DATE    AS date_day,
    'chat'                                            AS ai_feature,
    LOWER(ping_product_tier)                          AS plan,
    'External'                                        AS internal_or_external,
    ping_deployment_type                              AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'WAU'                                             AS metric
  FROM metric_prep 
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly'
  GROUP BY ALL


  UNION ALL

  SELECT
    ping_created_date_month::DATE       AS date_day,
    'chat'                              AS ai_feature,
    LOWER(ping_product_tier)            AS plan,
    'All'                               AS internal_or_external,
    ping_deployment_type                AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'MAU'                               AS metric
  FROM
   metric_prep 
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_monthly'
  GROUP BY ALL

  UNION ALL

  SELECT
    DATE_TRUNC(WEEK, ping_created_date_week)::DATE  AS date_day,
    'chat'                                          AS ai_feature,
    LOWER(ping_product_tier)                        AS plan,
    'All'                                           AS internal_or_external,
    ping_deployment_type                            AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'WAU'                                           AS metric
  FROM metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly'
  GROUP BY ALL
  UNION ALL

  SELECT
    ping_created_date_month::DATE       AS date_day,
    'chat'                              AS ai_feature,
    'All'                               AS plan,
    'All'                               AS internal_or_external,
    'All'                               AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'MAU'                               AS metric
  FROM
    metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_monthly'
  GROUP BY ALL

  UNION ALL

  SELECT
    DATE_TRUNC(WEEK, ping_created_date_week)::DATE    AS date_day,
    'chat'                                            AS ai_feature,
    'All'                                             AS plan,
    'All'                                             AS internal_or_external,
    'All'                                             AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'WAU'                                             AS metric
  FROM metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly'
  GROUP BY ALL

  -- START OF ALL COMBINATIONS

  UNION ALL

  SELECT
    ping_created_date_month::DATE       AS date_day,
    'chat'                              AS ai_feature,
    LOWER(ping_product_tier)            AS plan,
    'External'                          AS internal_or_external,
    'All'                               AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'MAU'                               AS metric
  FROM metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_monthly'
  GROUP BY ALL

  UNION ALL

  SELECT
    DATE_TRUNC(WEEK, ping_created_date_week)::DATE    AS date_day,
    'chat'                                            AS ai_feature,
    LOWER(ping_product_tier)                          AS plan,
    'External'                                        AS internal_or_external,
    'All'                                             AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'WAU'                                             AS metric
  FROM metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly'
  GROUP BY ALL

  UNION ALL

  SELECT
    DATE_TRUNC(WEEK, ping_created_date_week)::DATE    AS date_day,
    'chat'                                            AS ai_feature,
    LOWER(ping_product_tier)                          AS plan,
    'All'                                             AS internal_or_external,
    'All'                                             AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'WAU'                                             AS metric
  FROM metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly'
  GROUP BY ALL

  UNION ALL

  SELECT
    ping_created_date_month::DATE         AS date_day,
    'chat'                                AS ai_feature,
    'All'                                 AS plan,
    'External'                            AS internal_or_external,
    ping_deployment_type                  AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'MAU'                                 AS metric
  FROM metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_monthly'
  GROUP BY ALL

  UNION ALL

  SELECT
    DATE_TRUNC(WEEK, ping_created_date_week)::DATE    AS date_day,
    'chat'                                            AS ai_feature,
    'All'                                             AS plan,
    'External'                                        AS internal_or_external,
    ping_deployment_type                              AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'WAU'                                             AS metric
  FROM metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly'
  GROUP BY ALL
  UNION ALL

  SELECT
    ping_created_date_month::DATE       AS date_day,
    'chat'                              AS ai_feature,
    'All'                               AS plan,
    'All'                               AS internal_or_external,
    ping_deployment_type                AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'MAU'                               AS metric
  FROM metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_monthly'
  GROUP BY ALL

  UNION ALL

  SELECT
    DATE_TRUNC(WEEK, ping_created_date_week)::DATE    AS date_day,
    'chat'                                            AS ai_feature,
    'All'                                             AS plan,
    'All'                                             AS internal_or_external,
    ping_deployment_type                              AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'WAU'                                             AS metric
  FROM metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly'
  GROUP BY ALL

  UNION ALL

  SELECT
    ping_created_date_month::DATE       AS date_day,
    'chat'                              AS ai_feature,
    'All'                               AS plan,
    'External'                          AS internal_or_external,
    'All'                               AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'MAU'                               AS metric
  FROM metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_monthly'
  GROUP BY ALL

  UNION ALL

  SELECT
    DATE_TRUNC(WEEK, ping_created_date_week)::DATE    AS date_day,
    'chat'                                            AS ai_feature,
    'All'                                             AS plan,
    'External'                                        AS internal_or_external,
    'All'                                             AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'WAU'                                             AS metric
  FROM metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly'
  GROUP BY ALL

  UNION ALL

  SELECT
    DATE_TRUNC(MONTH, ping_created_date_month)::DATE    AS date_day,
    'chat'                                              AS ai_feature,
    LOWER(ping_product_tier)                            AS plan,
    'External'                                          AS internal_or_external,
    'All'                                               AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'MAU'                                               AS metric
  FROM metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_monthly'
  GROUP BY ALL

  UNION ALL

  SELECT
    DATE_TRUNC(MONTH, ping_created_date_month)::DATE    AS date_day,
    'chat'                                              AS ai_feature,
    LOWER(ping_product_tier)                            AS plan,
    'All'                                               AS internal_or_external,
    'All'                                               AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'MAU'                                               AS metric
  FROM metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_monthly'
  GROUP BY ALL

  UNION ALL

  SELECT
    DATE_TRUNC(WEEK, ping_created_at)::DATE     AS date_day,
    'chat'                                      AS ai_feature,
    LOWER(ping_product_tier)                    AS plan,
    'All'                                       AS internal_or_external,
    'All'                                       AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT),
    'WAU'                                       AS metric
  FROM metric_prep
  WHERE metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly'
  GROUP BY ALL

  UNION ALL

  SELECT
    dim_date.date_day::DATE       AS date_day,
    metrics.event_label           AS ai_feature,
    metrics.plan,
    metrics.internal_or_external,
    'All'                         AS delivery_type,
    metrics.metric_value,
    metrics.metric
  FROM dim_date
  LEFT JOIN metrics 
    ON dim_date.date_day = metrics._date
  WHERE dim_date.date_day BETWEEN '2023-04-21' AND CURRENT_DATE
    AND metrics.plan = 'All'
    AND metrics.internal_or_external = 'All'
    AND metrics.event_label = 'chat'
    AND metrics.metric IN ('MAU', 'WAU')


),

dedup AS (

  SELECT
    date_day,
    ai_feature,
    plan,
    internal_or_external,
    delivery_type,
    SUM(metric_value) AS metric_value,
    metric
  FROM unify
  WHERE date_day < CURRENT_DATE()
    AND metric_value IS NOT NULL
    AND metric IS NOT NULL
  GROUP BY ALL

)

SELECT *
FROM dedup
