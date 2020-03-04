WITH snowplow_page_views_90 AS (
  
    SELECT *
    FROM {{ ref('snowplow_page_views_90') }}

), 

snowplow_sessions_90 AS (
  
    SELECT *
    FROM {{ ref('snowplow_sessions_90') }}

), 

saas_funnel_subscription_start_page AS (
  
    SELECT 
      TO_DATE(min_tstamp) AS page_view_date,
      page_view_in_session_index,
      snowplow_page_views_90.session_id,
      page_url_path,
      page_url_query,
      snowplow_page_views_90.referer_url_path,
      CASE
        WHEN (snowplow_page_views_90.referer_url_host = 'about.gitlab.com' AND page_url_path = '/subscriptions/new'
           AND RLIKE(page_url_query, '(.)*plan_id=(2c92a0ff5a840412015aa3cde86f2ba6|2c92a0fd5a840403015aa6d9ea2c46d6|2c92a0fc5a83f01d015aa6db83c45aac)(.)*'))
        THEN 'SaaS Subscription Page'
          ELSE 'Others'
      END AS page_type
    FROM snowplow_page_views_90
    LEFT JOIN snowplow_sessions_90
      ON snowplow_page_views_90.session_id = snowplow_sessions_90.session_id
    WHERE  (snowplow_page_views_90.referer_url_host = 'about.gitlab.com' AND page_url_path = '/subscriptions/new'
      AND RLIKE(page_url_query, '(.)*plan_id=(2c92a0ff5a840412015aa3cde86f2ba6|2c92a0fd5a840403015aa6d9ea2c46d6|2c92a0fc5a83f01d015aa6db83c45aac)(.)*'))
      
)

, saas_funnel_subscription_success_page AS (
  
    SELECT *
    FROM snowplow_page_views_90
    WHERE (RLIKE(snowplow_page_views_90.page_url_path, '/subscriptions/([a-zA-Z0-9\-]{1,})/success/create_subscription') 
      AND RLIKE(snowplow_page_views_90.referer_url_query, '(.)*plan_id=(2c92a0ff5a840412015aa3cde86f2ba6|2c92a0fd5a840403015aa6d9ea2c46d6|2c92a0fc5a83f01d015aa6db83c45aac)(.)*'))
)

, joined AS (
  
    SELECT 
        saas_funnel_subscription_start_page.session_id,
        saas_funnel_subscription_start_page.min_tstamp,
        saas_funnel_subscription_start_page.session_id IS NOT NULL AS subscription_funnel_start_page,
        saas_funnel_subscription_success_page.session_id IS NOT NULL AS subscription_success_page
    FROM saas_funnel_subscription_start_page
    LEFT JOIN saas_funnel_subscription_success_page 
        ON saas_funnel_subscription_start_page.session_id = saas_funnel_subscription_success_page.session_id

)

SELECT *
FROM joined
