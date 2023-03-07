WITH find_current_ads AS (
  SELECT
    *,
    MAX(
      UPDATED_TIME
    ) OVER (PARTITION BY ad_id ORDER BY UPDATED_TIME DESC) AS latest_update,
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
    ) OVER (PARTITION BY creative_id ORDER BY _FIVETRAN_SYNCED DESC) AS latest_update,
    latest_update = _FIVETRAN_SYNCED AS is_latest
  FROM {{ ref('facebook_ads_creative_history_source') }}

),current_creatives AS (
  SELECT
    *
  FROM find_current_creatives
  WHERE is_latest

), basic_all_levels as (
    select * FROM {{ ref('facebook_ads_basic_all_levels_source') }}
)

select
    /* Account Info */
    basic_all_levels.account_id,
    
    /* Campaign Info */
    current_ads.campaign_id,
    basic_all_levels.campaign_name,
    basic_all_levels.adset_name,
    current_ads.name              as ad_name,
    current_ads.status            as ad_status,
    

    /* Creative Info */

    current_creatives.name        as creative_name,
    current_creatives.OBJECT_TYPE as creative_type,
    current_creatives.status      as creative_status,
    current_creatives.page_link,
    current_creatives.body        as text_ad_text,
    current_creatives.title       as text_ad_title,
    
    /* Creative Stats */
    basic_all_levels.date         as campaign_day,
    basic_all_levels.IMPRESSIONS,
    basic_all_levels.INLINE_LINK_CLICKS,
    basic_all_levels.spend 
FROM basic_all_levels
    left join current_ads on basic_all_levels.ad_id = current_ads.ad_id
    left join current_creatives on current_ads.CREATIVE_ID = current_creatives.CREATIVE_ID