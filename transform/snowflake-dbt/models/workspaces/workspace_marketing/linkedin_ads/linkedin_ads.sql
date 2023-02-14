WITH find_current_campaigns AS (
  SELECT
    *,
    MAX(
      LAST_MODIFIED_TIME
    ) OVER (PARTITION BY id ORDER BY LAST_MODIFIED_TIME DESC) AS latest_update,
    latest_update = LAST_MODIFIED_TIME AS is_latest
  FROM RAW.LINKEDIN_ADS.CAMPAIGN_HISTORY
),current_campaigns AS (
  SELECT
    *
  FROM find_current_campaigns
  WHERE is_latest
), find_current_creatives AS (
  SELECT
    *,
    MAX(
      LAST_MODIFIED_TIME
    ) OVER (PARTITION BY id ORDER BY LAST_MODIFIED_TIME DESC) AS latest_update,
    latest_update = LAST_MODIFIED_TIME AS is_latest
  FROM RAW.LINKEDIN_ADS.CREATIVE_HISTORY
),current_creatives AS (
  SELECT
    *
  FROM find_current_creatives
  WHERE is_latest
), creative_stats as (
  SELECT
    *
  FROM RAW.LINKEDIN_ADS.AD_ANALYTICS_BY_CREATIVE
)

SELECT
    
    /* Account Info */
     current_campaigns.account_id,
    /* Campaign Info */
     
    current_campaigns.id                  as campaign_id,
    current_campaigns.name                as campaign_name,
    current_campaigns.status              as campaign_status,
    current_campaigns.RUN_SCHEDULE_END    as campaign_end_date,
    current_campaigns.RUN_SCHEDULE_START  as campaign_start_date,
    current_campaigns.OBJECTIVE_TYPE      as ad_type,

    /* Creative Info */

    current_creatives.type                as creative_type,
    current_creatives.status              as creative_status,
    current_creatives.click_uri,
    current_creatives.text_ad_text,
    current_creatives.text_ad_title,
    
    /* Creative Stats */
    creative_stats.day as campaign_day,
    creative_stats.impressions,
    creative_stats.clicks,
    creative_stats.ONE_CLICK_LEADS as linkedin_leads,
    creative_stats.cost_in_usd
from
current_campaigns
    left join current_creatives on current_campaigns.id = current_creatives.CAMPAIGN_ID
    left join creative_stats on current_creatives.id = creative_stats.CREATIVE_ID