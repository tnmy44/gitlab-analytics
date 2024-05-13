{{ config(
    materialized='view',
    tags=["mnpi_exception", "product"]
) }}

WITH prep AS 
(
SELECT
    e.event_label,
    e.plan_name_modified AS plan_name,
    CASE 
    WHEN gsc_is_gitlab_team_member IN ('false', 'e08c592bd39b012f7c83bbc0247311b238ee1caa61be28ccfd412497290f896a') THEN 'External' 
    WHEN gsc_is_gitlab_team_member IN ('true','5616b37fa230003bc8510af409bf3f5970e6d5027cc282b0ab3080700d92e7ad') THEN 'Internal'
    ELSE 'Unknown' END AS internal_or_external,
    e.behavior_date,
    e.gsc_pseudonymized_user_id,
    DATE_TRUNC(WEEK,e.behavior_date) AS current_week,
    DATE_TRUNC(WEEK,DATEADD(DAY,7,e.behavior_date)) AS next_week,
    DATE_TRUNC(MONTH,e.behavior_date) AS current_month,
    DATE_TRUNC(MONTH,DATEADD(MONTH,1,e.behavior_date)) AS next_month
  FROM {{ ref('mart_behavior_structured_event') }} e
  WHERE 
    event_action = 'execute_llm_method'
    AND
    behavior_date BETWEEN '2023-04-21' AND CURRENT_DATE 
    AND
    event_category = 'Llm::ExecuteMethodService'
    AND 
    event_label != 'code_suggestions'
         
  ), WEEK_all AS
(
SELECT
DATE_TRUNC(WEEK,e.behavior_date) AS _date,
e.event_label,
'All' AS plan,
'All' AS internal_or_external,
COUNT(DISTINCT e.gsc_pseudonymized_user_id) AS user_count
FROM
prep e 
GROUP BY ALL
), WEEK_add_plan AS
(
SELECT
DATE_TRUNC(WEEK,e.behavior_date) AS _date,
e.event_label,
e.plan_name,
'All',
COUNT(DISTINCT e.gsc_pseudonymized_user_id) AS user_count
FROM
prep e 
GROUP BY ALL
), WEEK_split AS
(
SELECT
DATE_TRUNC(WEEK,e.behavior_date) AS _date,
e.event_label,
e.plan_name,
e.internal_or_external,
COUNT(DISTINCT e.gsc_pseudonymized_user_id) AS user_count
FROM
prep e 
GROUP BY ALL
), WEEK_int_ext AS
(
SELECT
DATE_TRUNC(WEEK,e.behavior_date) AS _date,
e.event_label,
'All',
e.internal_or_external,
COUNT(DISTINCT e.gsc_pseudonymized_user_id) AS user_count
FROM
prep e 
GROUP BY ALL
), wau AS 
(
SELECT
*
FROM
WEEK_all
WHERE
_date < DATE_TRUNC(WEEK,CURRENT_DATE())
UNION ALL 
SELECT
*
FROM
WEEK_add_plan
WHERE
_date < DATE_TRUNC(WEEK,CURRENT_DATE())
UNION ALL 
SELECT
*
FROM
WEEK_split
WHERE
_date < DATE_TRUNC(WEEK,CURRENT_DATE())
UNION ALL 
SELECT
*
FROM
WEEK_int_ext
WHERE
_date < DATE_TRUNC(WEEK,CURRENT_DATE())

), day_all AS
(
SELECT
DATE_TRUNC(DAY,e.behavior_date) AS _date,
e.event_label,
'All' AS plan,
'All' AS internal_or_external,
COUNT(DISTINCT e.gsc_pseudonymized_user_id) AS metric_value
FROM
prep e 
GROUP BY ALL
), day_add_plan AS
(
SELECT
DATE_TRUNC(DAY,e.behavior_date) AS _date,
e.event_label,
e.plan_name,
'All',
COUNT(DISTINCT e.gsc_pseudonymized_user_id) AS user_count
FROM
prep e 
GROUP BY ALL
), day_split AS
(
SELECT
DATE_TRUNC(DAY,e.behavior_date) AS _date,
e.event_label,
e.plan_name,
e.internal_or_external,
COUNT(DISTINCT e.gsc_pseudonymized_user_id) AS user_count
FROM
prep e 
GROUP BY ALL
), day_int_ext AS
(
SELECT
DATE_TRUNC(DAY,e.behavior_date) AS _date,
e.event_label,
'All',
e.internal_or_external,
COUNT(DISTINCT e.gsc_pseudonymized_user_id) AS user_count
FROM
prep e 
GROUP BY ALL
), dau AS 
(
SELECT
*
FROM
day_all
UNION ALL 
SELECT
*
FROM
day_add_plan
UNION ALL 
SELECT
*
FROM
day_split
UNION ALL 
SELECT
*
FROM
day_int_ext

), MONTH_all AS
(
SELECT
DATE_TRUNC(MONTH,e.behavior_date) AS _date,
e.event_label,
'All' AS plan,
'All' AS internal_or_external,
COUNT(DISTINCT e.gsc_pseudonymized_user_id) AS user_count
FROM
prep e 
GROUP BY ALL
), MONTH_add_plan AS
(
SELECT
DATE_TRUNC(MONTH,e.behavior_date) AS _date,
e.event_label,
e.plan_name,
'All',
COUNT(DISTINCT e.gsc_pseudonymized_user_id) AS user_count
FROM
prep e 
GROUP BY ALL
), MONTH_split AS
(
SELECT
DATE_TRUNC(MONTH,e.behavior_date) AS _date,
e.event_label,
e.plan_name,
e.internal_or_external,
COUNT(DISTINCT e.gsc_pseudonymized_user_id) AS user_count
FROM
prep e 
GROUP BY ALL
), MONTH_int_ext AS
(
SELECT
DATE_TRUNC(MONTH,e.behavior_date) AS _date,
e.event_label,
'All',
e.internal_or_external,
COUNT(DISTINCT e.gsc_pseudonymized_user_id) AS user_count
FROM
prep e 
GROUP BY ALL
), mau AS 
(
SELECT
*
FROM
MONTH_all
WHERE
_date < DATE_TRUNC(MONTH,CURRENT_DATE())
UNION ALL 
SELECT
*
FROM
MONTH_add_plan
WHERE
_date < DATE_TRUNC(MONTH,CURRENT_DATE())
UNION ALL 
SELECT
*
FROM
MONTH_split
WHERE
_date < DATE_TRUNC(MONTH,CURRENT_DATE())
UNION ALL 
SELECT
*
FROM
MONTH_int_ext
WHERE
_date < DATE_TRUNC(MONTH,CURRENT_DATE())

), weekly_retention_grouped AS 
(
SELECT
e.current_week,
e.event_label,
e.plan_name,
e.internal_or_external,
(COUNT(DISTINCT e2.gsc_pseudonymized_user_id)/COUNT(DISTINCT e.gsc_pseudonymized_user_id)) AS retention_rate
FROM
prep e
LEFT JOIN 
prep e2 ON e2.event_label = e.event_label AND e2.gsc_pseudonymized_user_id = e.gsc_pseudonymized_user_id AND e2.current_week = e.next_week
WHERE
e.next_week < DATE_TRUNC(WEEK,CURRENT_DATE())
GROUP BY ALL
HAVING 
COUNT(DISTINCT e.gsc_pseudonymized_user_id) > 0
), monthly_retention_grouped AS 
(
SELECT
e.current_month,
e.event_label,
e.plan_name,
e.internal_or_external,
(COUNT(DISTINCT e2.gsc_pseudonymized_user_id)/COUNT(DISTINCT e.gsc_pseudonymized_user_id)) AS retention_rate
FROM
prep e
LEFT JOIN 
prep e2 ON e2.event_label = e.event_label AND e2.gsc_pseudonymized_user_id = e.gsc_pseudonymized_user_id AND e2.current_month = e.next_month
WHERE
e.next_month < DATE_TRUNC(MONTH,CURRENT_DATE())
GROUP BY ALL
HAVING 
COUNT(DISTINCT e.gsc_pseudonymized_user_id) > 0
), weekly_retention_no_plan AS 
(
SELECT
e.current_week,
e.event_label,
'All',
e.internal_or_external,
(COUNT(DISTINCT e2.gsc_pseudonymized_user_id)/COUNT(DISTINCT e.gsc_pseudonymized_user_id)) AS retention_rate
FROM
prep e
LEFT JOIN 
prep e2 ON e2.event_label = e.event_label AND e2.gsc_pseudonymized_user_id = e.gsc_pseudonymized_user_id AND e2.current_week = e.next_week
WHERE
e.next_week < DATE_TRUNC(WEEK,CURRENT_DATE())
GROUP BY ALL
HAVING 
COUNT(DISTINCT e.gsc_pseudonymized_user_id) > 0
), monthly_retention_no_plan AS 
(
SELECT
e.current_month,
e.event_label,
'All',
e.internal_or_external,
(COUNT(DISTINCT e2.gsc_pseudonymized_user_id)/COUNT(DISTINCT e.gsc_pseudonymized_user_id)) AS retention_rate
FROM
prep e
LEFT JOIN 
prep e2 ON e2.event_label = e.event_label AND e2.gsc_pseudonymized_user_id = e.gsc_pseudonymized_user_id AND e2.current_month = e.next_month
WHERE
e.next_month < DATE_TRUNC(MONTH,CURRENT_DATE())
GROUP BY ALL
HAVING 
COUNT(DISTINCT e.gsc_pseudonymized_user_id) > 0
)
, weekly_retention_no_intext AS 
(
SELECT
e.current_week,
e.event_label,
e.plan_name,
'All',
(COUNT(DISTINCT e2.gsc_pseudonymized_user_id)/COUNT(DISTINCT e.gsc_pseudonymized_user_id)) AS retention_rate
FROM
prep e
LEFT JOIN 
prep e2 ON e2.event_label = e.event_label AND e2.gsc_pseudonymized_user_id = e.gsc_pseudonymized_user_id AND e2.current_week = e.next_week
WHERE
e.next_week < DATE_TRUNC(WEEK,CURRENT_DATE())
GROUP BY ALL
HAVING 
COUNT(DISTINCT e.gsc_pseudonymized_user_id) > 0
), monthly_retention_no_intext AS 
(
SELECT
e.current_month,
e.event_label,
e.plan_name,
'All',
(COUNT(DISTINCT e2.gsc_pseudonymized_user_id)/COUNT(DISTINCT e.gsc_pseudonymized_user_id)) AS retention_rate
FROM
prep e
LEFT JOIN 
prep e2 ON e2.event_label = e.event_label AND e2.gsc_pseudonymized_user_id = e.gsc_pseudonymized_user_id AND e2.current_month = e.next_month
WHERE
e.next_month < DATE_TRUNC(MONTH,CURRENT_DATE())
GROUP BY ALL
HAVING 
COUNT(DISTINCT e.gsc_pseudonymized_user_id) > 0
), weekly_retention_all AS 
(
SELECT
e.current_week,
e.event_label,
'All',
'All',
(COUNT(DISTINCT e2.gsc_pseudonymized_user_id)/COUNT(DISTINCT e.gsc_pseudonymized_user_id)) AS retention_rate
FROM
prep e
LEFT JOIN 
prep e2 ON e2.event_label = e.event_label AND e2.gsc_pseudonymized_user_id = e.gsc_pseudonymized_user_id AND e2.current_week = e.next_week
WHERE
e.next_week < DATE_TRUNC(WEEK,CURRENT_DATE())
GROUP BY ALL
HAVING 
COUNT(DISTINCT e.gsc_pseudonymized_user_id) > 0
), monthly_retention_all AS 
(
SELECT
e.current_month,
e.event_label,
'All',
'All',
(COUNT(DISTINCT e2.gsc_pseudonymized_user_id)/COUNT(DISTINCT e.gsc_pseudonymized_user_id)) AS retention_rate
FROM
prep e
LEFT JOIN 
prep e2 ON e2.event_label = e.event_label AND e2.gsc_pseudonymized_user_id = e.gsc_pseudonymized_user_id AND e2.current_month = e.next_month
WHERE
e.next_month < DATE_TRUNC(MONTH,CURRENT_DATE())
GROUP BY ALL
HAVING 
COUNT(DISTINCT e.gsc_pseudonymized_user_id) > 0
)
, retentions AS 
(
SELECT
*,
'Weekly Retention' AS metric
FROM
weekly_retention_grouped
UNION ALL 
SELECT
*,
'Monthly Retention' AS metric
FROM
monthly_retention_grouped
UNION ALL 
SELECT
*,
'Weekly Retention' AS metric
FROM
weekly_retention_no_plan
UNION ALL 
SELECT
*,
'Monthly Retention' AS metric
FROM
monthly_retention_no_plan
UNION ALL 
SELECT
*,
'Weekly Retention' AS metric
FROM
weekly_retention_no_intext
UNION ALL 
SELECT
*,
'Monthly Retention' AS metric
FROM
monthly_retention_no_intext
UNION ALL 
SELECT
*,
'Weekly Retention' AS metric
FROM
weekly_retention_all
UNION ALL 
SELECT
*,
'Monthly Retention' AS metric
FROM
monthly_retention_all


)
, metrics AS 
(
SELECT
*,
'DAU' AS metric
FROM
dau
UNION ALL
SELECT
*,
'WAU' AS metric
FROM
wau
WHERE
_date < DATE_TRUNC(WEEK,CURRENT_DATE)
UNION ALL
SELECT
*,
'MAU' AS metric
FROM
mau
WHERE
_date < DATE_TRUNC(MONTH,CURRENT_DATE)
UNION ALL
SELECT
*
FROM
retentions

), unify AS 
(
SELECT
d.date_day,
metrics.event_label AS ai_feature,
metrics.plan,
metrics.internal_or_external,
'Gitlab.com' AS delivery_type,
metrics.metric_value,
metrics.metric
FROM
PROD.common.dim_date d 
LEFT JOIN metrics ON d.date_day = metrics._date 
WHERE
d.date_day BETWEEN '2023-04-21' AND CURRENT_DATE

UNION ALL 

SELECT 
p.ping_created_date_month AS date_day,
'chat' AS ai_feature,
p.ping_product_tier AS plan,
'External' AS internal_or_external,
p.ping_deployment_type,
COALESCE(p.metric_value,0)::INT,
'MAU' AS metric
FROM 
PROD.common_mart.mart_ping_instance_metric p
WHERE
  p.metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_monthly'
  AND p.major_minor_version_id >= 1611
  AND p.metric_value > 0
  AND p.is_last_ping_of_month = TRUE
  AND p.ping_created_date_month > '2024-01-01'
  AND p.ping_deployment_type != 'Gitlab.com'
  AND p.ping_deployment_type != 'GitLab.com'

UNION ALL 

SELECT 
p.ping_created_date_week AS date_day,
'chat' AS ai_feature,
p.ping_product_tier AS plan,
'External' AS internal_or_external,
p.ping_deployment_type AS delivery_type,
COALESCE(p.metric_value,0)::INT,
'WAU' AS metric
FROM 
PROD.common_mart.mart_ping_instance_metric p
WHERE
  p.metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly'
  AND p.major_minor_version_id >= 1611
  AND p.metric_value > 0
  AND p.is_last_ping_of_week = TRUE
  AND p.ping_created_date_month > '2024-01-01'
  AND p.ping_deployment_type != 'Gitlab.com'
  AND p.ping_deployment_type != 'GitLab.com'
), dedup AS 
(
SELECT
u.date_day,
u.ai_feature,
u.plan,
u.internal_or_external,
u.delivery_type,
u.metric_value,
u.metric
FROM
unify u 
WHERE
u.date_day < CURRENT_DATE()
AND
u.metric_value IS NOT NULL
AND
u.metric IS NOT NULL
AND
u.ai_feature !='chat'
AND 
(u.metric!='WAU' OR u.metric!='MAU')
AND
u.plan != 'All'
AND
u.internal_or_external != 'All'
AND
u.delivery_type != 'All'


UNION ALL 

SELECT
u.date_day,
u.ai_feature,
u.plan,
u.internal_or_external,
u.delivery_type,
SUM(u.metric_value) + SUM(u2.metric_value),
u.metric
FROM
unify u 
LEFT JOIN unify u2 
  ON u.date_day = u2.date_day 
  AND u.ai_feature = u2.ai_features
  AND u.plan = u.plan
  AND u.internal_or_external = u2.internal_or_external
  AND u2.delivery_type = 'Self-Managed'
  AND u.metric = u2.metric
  WHERE
u.date_day < CURRENT_DATE()
AND
u.metric_value IS NOT NULL
AND
u.ai_feature ='chat'
AND 
u.metric='WAU'
AND
u.plan = 'All'
AND
u.internal_or_external = 'All'
AND
u.delivery_type = 'All'

UNION ALL 

SELECT
u.date_day,
u.ai_feature,
u.plan,
u.internal_or_external,
u.delivery_type,
SUM(u.metric_value) + SUM(u2.metric_value),
u.metric
FROM
unify u 
LEFT JOIN unify u2 
  ON u.date_day = u2.date_day 
  AND u.ai_feature = u2.ai_features
  AND u.plan = u.plan
  AND u.internal_or_external = u2.internal_or_external
  AND u2.delivery_type = 'Self-Managed'
  AND u.metric = u2.metric
WHERE
u.date_day < CURRENT_DATE()
AND
u.metric_value IS NOT NULL
AND
u.ai_feature ='chat'
AND 
u.metric='MAU'
AND
u.plan = 'All'
AND
u.internal_or_external = 'All'
AND
u.delivery_type = 'All'
)

SELECT
*
FROM 
dedup
