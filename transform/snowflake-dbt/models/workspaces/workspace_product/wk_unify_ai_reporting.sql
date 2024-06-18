{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('dim_date', 'dim_date')
]) }},

dotcom_prep AS (

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

), ai_features AS (
SELECT
DISTINCT 
event_label 
FROM 
dotcom_prep
UNION ALL 
SELECT
'All'
), plans AS 
(
SELECT
DISTINCT 
CASE
WHEN plan_name = 'opensource' THEN 'Free'
WHEN plan_name = 'free' THEN 'Free'
WHEN plan_name = 'premium' THEN 'Premium'
WHEN plan_name = 'ultimate_trial' THEN 'Trial'
WHEN plan_name = 'ultimate' THEN 'Ultimate'
WHEN plan_name = 'All' THEN 'All'
WHEN plan_name = 'ultimate_trial_paid_customer' THEN 'Trial'
WHEN plan_name = 'premium_trial' THEN 'Trial'
WHEN plan_name = 'starter' THEN 'Starter'
WHEN plan_name = 'default' THEN 'Free'
END AS plan 
FROM 
dotcom_prep
UNION ALL 
SELECT
'All'
), int_ext_all AS 
(
SELECT
DISTINCT 
internal_or_external 
FROM 
dotcom_prep
UNION ALL 
SELECT
'All'
), prep AS 
(
SELECT

p.gsc_pseudonymized_user_id,
f.event_label,
i.internal_or_external,
plans.plan AS plan_name,
p.behavior_date,
p.current_week,
p.next_week,
p.current_month,
p.next_month
FROM
dotcom_prep p 
LEFT JOIN 
ai_features f 
ON 
p.event_label = f.event_label OR f.event_label = 'All' 
LEFT JOIN 
int_ext_all i 
ON 
p.internal_or_external = i.internal_or_external OR i.internal_or_external = 'All' 
LEFT JOIN 
plans ON plans.plan = p.plan_name OR plans.plan = 'All'
), DAU AS (

  SELECT
    behavior_date           AS _date,
    event_label,
    plan_name,
    internal_or_external,
    'DAU' AS metric,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS metric_value
  FROM prep
  WHERE
  behavior_date < CURRENT_DATE
  GROUP BY ALL

), WAU AS (

  SELECT
    DATE_TRUNC(WEEK, behavior_date)           AS _date,
    event_label,
    plan_name,
    internal_or_external,
    'WAU' AS metric,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS user_count
  FROM prep
  WHERE
  DATE_TRUNC(WEEK, behavior_date) < DATE_TRUNC(WEEK, CURRENT_DATE)  
  GROUP BY ALL

), MAU AS (

  SELECT
    DATE_TRUNC(MONTH, behavior_date)           AS _date,
    event_label,
    plan_name,
    internal_or_external,
    'MAU' AS metric,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS user_count
  FROM prep
  WHERE
  DATE_TRUNC(MONTH, behavior_date) < DATE_TRUNC(MONTH, CURRENT_DATE) 
  GROUP BY ALL

), 
weekly_retention_grouped AS (
  SELECT
    prep.current_week,
    prep.event_label,
    prep.plan_name,
    prep.internal_or_external,
    'Weekly Retention' AS metric,
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
    'Monthly Retention' AS metric,
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

metrics AS (
  
  SELECT
    *
  FROM DAU
  
  UNION ALL

  SELECT
    *
  FROM WAU
  
  UNION ALL

  SELECT
    *
  FROM MAU
  
  UNION ALL

  SELECT *
  FROM weekly_retention_grouped

  UNION ALL

  SELECT *
  FROM monthly_retention_grouped
), metric_prep AS (
  
  SELECT 
  *
  FROM
    {{ ref('mart_ping_instance_metric') }}
  WHERE major_minor_version_id >= 1611
    AND metric_value > 0
    AND is_last_ping_of_month = TRUE
    AND ping_created_date_month > '2024-01-01'
    AND ping_deployment_type != 'GitLab.com'
    AND  metrics_path IN  (
    'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_monthly' ,
    'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly'
    )

), sm_expanded AS 
(
  SELECT
    ping_created_date_month::DATE       AS date_day,
    f.event_label AS ai_feature,
    ping_product_tier            AS plan,
    i.internal_or_external,
    ping_deployment_type                AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT) AS metric_value,
    CASE 
    WHEN metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_monthly' THEN 'MAU'
    WHEN metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly' THEN 'WAU'
    ELSE NULL END
                                    AS metric
  FROM metric_prep
  LEFT JOIN 
  ai_features f 
  ON 
  f.event_label = 'chat' OR f.event_label = 'All' 
  LEFT JOIN 
  int_ext_all i 
  ON 
  i.internal_or_external = 'External' OR i.internal_or_external = 'All' 
  LEFT JOIN 
  plans ON plans.plan = metric_prep.ping_product_tier OR plans.plan = 'All'
  GROUP BY ALL
),
unify AS (

  SELECT
    dim_date.date_day::DATE       AS date_day,
    metrics.event_label           AS ai_feature,
    metrics.plan_name,
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
    dim_date.date_day::DATE       AS date_day,
    metrics.event_label           AS ai_feature,
    metrics.plan_name,
    metrics.internal_or_external,
    'All'                  AS delivery_type,
    metrics.metric_value,
    metrics.metric
  FROM dim_date
  LEFT JOIN metrics 
    ON dim_date.date_day = metrics._date
  WHERE dim_date.date_day BETWEEN '2023-04-21' AND CURRENT_DATE

  UNION ALL

  SELECT
    date_day,
    ai_feature,
    plan,
    internal_or_external,
    delivery_type,
    metric_value
    metric
  FROM sm_expanded

  UNION ALL
  
  SELECT
    date_day,
    ai_feature,
    plan,
    internal_or_external,
    'All' AS delivery_type,
    metric_value
    metric
  FROM sm_expanded

),

dedup AS (

  SELECT
    date_day,
    ai_feature,
    plan_name,
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
