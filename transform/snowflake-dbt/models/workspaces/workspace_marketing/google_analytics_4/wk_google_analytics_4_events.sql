{{ config({
    "materialized": "view",
    })
}}

SELECT 
    google_analytics_4_events_source.*,
    CASE
        WHEN LOWER(medium) IS NULL AND LOWER(source) IS NULL
        THEN 'Direct'
        WHEN LOWER(medium) LIKE '%email%' 
            OR LOWER(source) LIKE '%email%'
        THEN 'Email'
        WHEN LOWER(medium) LIKE '%cpm%' 
            OR LOWER(medium) LIKE '%display%'
            OR LOWER(medium) = 'banner'
            OR (LOWER(source) = 'bsa' AND LOWER(medium) = 'banner')
            OR (LOWER(source) = 'google' AND LOWER(medium) = 'cpc' AND LOWER(campaign) LIKE '%youtube%')
            OR (LOWER(source) = 'google' AND LOWER(medium) = 'cpc' AND LOWER(campaign) LIKE '%display%')
            OR (LOWER(source) = 'bidtellect' AND LOWER(medium) = 'cpc')
        THEN 'Display'
        WHEN (LOWER(source) = 'capterra' AND LOWER(medium) = 'cpc')
            OR (LOWER(source) = 'getapp' AND LOWER(medium) = 'cpc')
            OR (LOWER(source) IS NOT NULL AND LOWER(medium) LIKE '%cpc%')
        THEN 'Paid Search'
        WHEN LOWER(medium) LIKE '%paidsocial%'
        THEN 'Paid Social'
        WHEN LOWER(medium) = 'organic'
            OR (LOWER(medium) = 'referral' AND LOWER(source) IN ('yandex', 'bing', 'yahoo', 'msn', 'sogou', 'baidu', 'lens.google.com', 'naver.com', 'so.com', 'news.google.com', 'search.google.com', 'poczta.onet.pl', 'websearch.rakuten.co.jp', 'duckduckgo.com'))
        THEN 'Organic Search' 
        WHEN (LOWER(medium) = 'referral' AND LOWER(source) IN ('amazon', 'walmart', 'shopify', 'alibaba', 'stripe'))
        THEN 'Organic Shopping' 
        WHEN LOWER(medium) = 'social'
            OR (LOWER(medium) = 'referral' AND LOWER(source) IN ('reddit', 'linkedin', 'medium', 'ycombinator', 't.co', 'twitter', 'facebook', 'tencent', 'lnkd', 'sites.google.com', 'web.skype.com', 'instagram', 'stackoverflow', 'vk', 'zalo', 'groups.google.com', 'yammer', 'quora', 'meetup', 'messenger', 'blog.naver.com', 'netvibes', 'wordpress', 'glassdoor', 'dzone', 'slashdot', 'flipboard', 'blogspot', 'weibo', 'tripadvisor', 't.me', 'getpocket', 'whatsapp', 'hatena', 'livejournal'))
        THEN 'Organic Social' 
        WHEN (LOWER(source) LIKE '%youtube%' AND LOWER(medium) = 'referral')
            OR (LOWER(source) LIKE '%vimeo%' AND LOWER(medium) = 'referral')
            OR (LOWER(source) LIKE '%iqiyi%' AND LOWER(medium) = 'referral')
            OR (LOWER(source) LIKE '%youku%' AND LOWER(medium) = 'referral')
        THEN 'Organic Video'
        WHEN source IS NOT NULL AND LOWER(medium) = 'referral'
        THEN 'Referral'
        WHEN source IS NOT NULL
            OR LOWER(source) = 'other'
        THEN 'Unassigned'
        ELSE 'Missing Channel Grouping'
    END AS channel
FROM {{ ref('google_analytics_4_events_source') }}
