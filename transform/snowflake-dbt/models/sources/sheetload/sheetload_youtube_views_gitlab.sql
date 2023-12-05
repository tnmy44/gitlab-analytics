
WITH source AS (
  SELECT *
  FROM {{ source('sheetload', 'sheetload_youtube_views_gitlab') }}
), renamed AS (
  SELECT 
    video_id::VARCHAR       AS video_id,
    team::VARCHAR           AS team,
    account::VARCHAR        AS VARCHAR,
    playlist::VARCHAR       AS playlist,
    url::VARCHAR            as url,
    video_title::VARCHAR    as video_title,
    publication_date::VARCHAR            as publication_date,
    gitLab_publication_quarter::VARCHAR  as gitLab_publication_quarter,
    include_in_looker_report::VARCHAR    as include_in_looker_report
  FROM source
)
SELECT *
FROM renamed
WHERE video_id IS NOT NULL