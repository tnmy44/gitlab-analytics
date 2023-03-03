/*
    Each Platform hAS different ways they organize campaigns. 
    The table below is taken from Linkedin Docs that hAS an accurate comparison

    +-------+-------------+----------+----------------+
    | Level |   Google    | Facebook |    LinkedIn    |
    +-------+-------------+----------+----------------+
    |     1 | Campaign    | Campaign | Campaign Group |
    |     2 | Ad Group    | Ad Set   | Campaign       |
    |     3 | Ad Keyword* | Ad       | Creative       |
    +-------+-------------+----------+----------------+
    |     4 |             | Creative |                |

    Facebook hAS another level to get the destination url called creative.
    *Fivetran reanmes this layer to `Ad`

    This union query prefers the Facebook naming since its the most generic and 
    most marketers understand a `campaign` AS the top level.

*/

with union_sources AS (
    SELECT
    google_ads.ad_stats_date AS report_date,
    'google_ads' AS platform,

    google_ads.campaign_id,
    google_ads.campaign_name,
    google_ads.campaign_status,
    google_ads.campaign_end_date,
    google_ads.campaign_start_date,

    google_ads.ad_status,
    NULL                                           AS creative_type,
    LTRIM(RTRIM(google_ads.ad_final_urls,']'),'[') AS landing_page_url,

    
    sum(impressions)                               AS impressions,
    sum(clicks)                                    AS clicks,
    NULL                                           AS linkedin_leads,
    sum(cost_micros / 1000000)                     AS spend

  FROM
  PROD.WORKSPACE_MARKETING.GOOGLE_ADS
  GROUP BY 1,2,3,4,5,6,7,8,9,10

  union all

  SELECT
  linkedin_ads.day                    AS report_date,
  'linkedin_ads'                      AS platform,

  linkedin_ads.campaign_id,
  linkedin_ads.campaign_name,
  linkedin_ads.campaign_status,
  linkedin_ads.campaign_end_date,
  linkedin_ads.campaign_start_date,

  linkedin_ads.ad_status,
  linkedin_ads.creative_type,
  linkedin_ads.click_uri              AS landing_page_url,
  
  sum(impressions)                    AS impressions,
  sum(clicks)                         AS clicks,
  sum(linkedin_leads)                 AS linkedin_leads,
  sum(cost_in_usd)                    AS spend

  FROM
  PROD.WORKSPACE_MARKETING.LINKEDIN_ADS
  GROUP BY 1,2,3,4,5,6,7,8,9,10

  union all

  SELECT
  facebook_ads.day            AS report_date,
  'facebook_ads'              AS platform,

  facebook_ads.campaign_id,
  facebook_ads.campaign_name,
  NULL                        AS campaign_status,
  NULL                        AS campaign_end_date,
  NULL                        AS campaign_start_date,

  facebook_ads.ad_status,
  facebook_ads.creative_type,
  facebook_ads.page_link      AS landing_page_url,
  
  sum(impressions)            AS impressions,
  sum(inline_link_clicks)     AS clicks,
  NULL                        AS linkedin_leads,
  sum(spend)                  AS spend

  FROM
  PROD.WORKSPACE_MARKETING.FACEBOOK_ADS
  GROUP BY 1,2,3,4,5,6,7,8,9,10

), parse_utms AS (
  
  SELECT
  union_sources.*
    -- keywords,
  lower(CASE WHEN SPLIT_PART(SPLIT_PART(landing_page_url,'utm_medium=',2),'&',1)= ''
    THEN SPLIT_PART(SPLIT_PART(rpt_lead_to_revenue.BIZIBLE_LANDING_PAGE_RAW,'utm_medium=',2),'&',1)
   ELSE SPLIT_PART(SPLIT_PART(landing_page_url,'utm_medium=',2),'&',1) END) AS utm_medium,
  lower(CASE WHEN SPLIT_PART(SPLIT_PART(landing_page_url,'utm_source=',2),'&',1)= ''
    THEN SPLIT_PART(SPLIT_PART(rpt_lead_to_revenue.BIZIBLE_LANDING_PAGE_RAW,'utm_source=',2),'&',1)
   ELSE SPLIT_PART(SPLIT_PART(landing_page_url,'utm_source=',2),'&',1) END) AS utm_source,
  lower(CASE WHEN SPLIT_PART(SPLIT_PART(landing_page_url,'utm_budget=',2),'&',1)= ''
    THEN SPLIT_PART(SPLIT_PART(rpt_lead_to_revenue.BIZIBLE_LANDING_PAGE_RAW,'utm_budget=',2),'&',1)
   ELSE SPLIT_PART(SPLIT_PART(landing_page_url,'utm_budget=',2),'&',1) END) AS utm_budget,
  lower(CASE WHEN SPLIT_PART(SPLIT_PART(landing_page_url,'utm_campaign=',2),'&',1)= ''
    THEN SPLIT_PART(SPLIT_PART(rpt_lead_to_revenue.BIZIBLE_LANDING_PAGE_RAW,'utm_campaign=',2),'&',1)
   ELSE SPLIT_PART(SPLIT_PART(landing_page_url,'utm_campaign=',2),'&',1) END) AS utm_campaign,
  SPLIT_PART(utm_campaign,'_',1) AS utm_campaigncode,
  SPLIT_PART(utm_campaign,'_',2) AS utm_geo,
  SPLIT_PART(utm_campaign,'_',3) AS utm_targeting,
  SPLIT_PART(utm_campaign,'_',4) AS utm_ad_unit,
  SPLIT_PART(utm_campaign,'_',5) AS "utm_br/bn",
  SPLIT_PART(utm_campaign,'_',6) AS utm_matchtype,
  lower(CASE WHEN SPLIT_PART(SPLIT_PART(landing_page_url,'utm_content=',2),'&',1)= ''
    THEN SPLIT_PART(SPLIT_PART(rpt_lead_to_revenue.BIZIBLE_LANDING_PAGE_RAW,'utm_content=',2),'&',1)
   ELSE SPLIT_PART(SPLIT_PART(landing_page_url,'utm_content=',2),'&',1) END) AS utm_content,
  SPLIT_PART(utm_content,'_',1)  AS utm_contentcode,
  SPLIT_PART(utm_content,'_',2)  AS utm_team,
  SPLIT_PART(utm_content,'_',3)  AS utm_segment,
  SPLIT_PART(utm_content,'_',4)  AS utm_language
  FROM 
  union_sources

)
select
*
from
parse_utms