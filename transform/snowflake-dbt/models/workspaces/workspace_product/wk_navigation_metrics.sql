{{ config(
    materialized='incremental',
    tags=["mnpi_exception", "product"]
) }}

WITH alls AS (
SELECT
DATE_TRUNC(MONTH,p.page_view_start_at) AS page_view_month,
COUNT(DISTINCT p.gsc_pseudonymized_user_id) AS unique_users,
COUNT(DISTINCT p.session_id) AS sessions,
COUNT(1) AS total_pageviews
FROM 
{{ ref('fct_behavior_website_page_view') }} p
WHERE 
page_view_month < DATE_TRUNC(MONTH,CURRENT_DATE())
AND
p.page_view_start_at > '2022-06-01'

{% if is_incremental() %}

AND
page_view_month > (SELECT MAX(page_view_month) FROM {{this}}) -- goal is to add entire new months only. 

{% endif %}

GROUP BY 1
), news AS 
(
SELECT
DATE_TRUNC(MONTH,mr.derived_tstamp) AS page_view_month,
COUNT(DISTINCT mr.gsc_pseudonymized_user_id) AS users_count,
COUNT(DISTINCT mr.session_id) AS sessions,
COUNT(1) AS total_occurences
FROM
{{ ref('wk_rpt_product_navigation_base') }} mr
WHERE
mr.derived_tstamp > DATEADD(DAY,1,LAST_DAY(DATEADD(MONTH,-19,CURRENT_DATE())))
AND
mr.app_id IN ('gitlab.com', 'gitlab_customers')
AND
_month < DATE_TRUNC(MONTH,CURRENT_DATE())
AND
mr.derived_tstamp > '2022-06-01'
{% if is_incremental() %}

AND
_month > (SELECT MAX(page_view_month) FROM {{this}}) -- goal is to add entire new months only. 

{% endif %}
GROUP BY 
1
)

SELECT
alls.page_view_month,
news.users_count AS nav_users,
alls.unique_users,
news.sessions AS nav_sessions,
alls.sessions AS event_sessions,
news.total_occurences AS nav_pageviews,
alls.total_pageviews,
news.users_count/alls.unique_users AS percent_users_on_navigation,
news.sessions/alls.sessions AS ratio_navigations_to_sessions,
news.total_occurences/alls.total_pageviews AS ratio_nav_views_to_all
FROM
alls 
LEFT JOIN 
news ON alls.page_view_month = news._month
