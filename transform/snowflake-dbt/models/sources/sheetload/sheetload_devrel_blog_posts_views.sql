
WITH source AS (
  SELECT *
  FROM {{ source('sheetload', 'devrel_blog_posts_views') }}
), renamed AS (
  SELECT 
    publication_date::VARCHAR            as publication_date,
    gitLab_publication_quarter::VARCHAR  as gitLab_publication_quarter,
    url::VARCHAR     as url,
    author::VARCHAR  as author,
    team::VARCHAR    as team

  FROM source
)
SELECT *
FROM renamed
WHERE url IS NOT NULL