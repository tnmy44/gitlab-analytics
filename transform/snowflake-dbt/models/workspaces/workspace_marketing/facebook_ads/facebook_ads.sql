WITH find_current_ads AS (
  SELECT
    *,
    MAX(
      UPDATED_TIME
    ) OVER (PARTITION BY id ORDER BY UPDATED_TIME DESC) AS latest_update,
    latest_update = UPDATED_TIME AS is_latest
  FROM {{ ref('facebook_ads_ad_history_source') }}
),current_ads AS (
  SELECT
    *
  FROM find_current_ads
  WHERE is_latest
), find_current_creatives AS (
  SELECT
    *,
    MAX(
      _FIVETRAN_SYNCED
    ) OVER (PARTITION BY id ORDER BY _FIVETRAN_SYNCED DESC) AS latest_update,
    latest_update = _FIVETRAN_SYNCED AS is_latest
  FROM {{ ref('facebook_ads_creative_history_source') }}
),current_creatives AS (
  SELECT
    *
  FROM find_current_creatives
  WHERE is_latest
)
select
    /* Account Info */
     BASIC_ALL_LEVELS.account_id,
    
    /* Campaign Info */
    current_ads.campaign_id,
    BASIC_ALL_LEVELS.campaign_name,
    BASIC_ALL_LEVELS.adset_name,
    current_ads.name        as ad_name,
    current_ads.status      as ad_status,
    

    /* Creative Info */

    current_creatives.name        as creative_name,
    current_creatives.OBJECT_TYPE as creative_type,
    current_creatives.status      as creative_status,
    current_creatives.page_link,
    current_creatives.body        as text_ad_text,
    current_creatives.title       as text_ad_title,
    
    -- -- /* Creative Stats */
    BASIC_ALL_LEVELS.date as campaign_day,
    BASIC_ALL_LEVELS.IMPRESSIONS,
    BASIC_ALL_LEVELS.INLINE_LINK_CLICKS,
    BASIC_ALL_LEVELS.spend 
FROM {{ ref('facebook_ads_basic_all_levels_source') }}
    left join current_ads on BASIC_ALL_LEVELS.ad_id = current_ads.id
    left join current_creatives on current_ads.CREATIVE_ID = current_creatives.id