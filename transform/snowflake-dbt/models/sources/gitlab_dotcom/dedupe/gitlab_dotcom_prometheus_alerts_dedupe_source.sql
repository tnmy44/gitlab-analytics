
  SELECT *
  FROM {{ source('gitlab_dotcom', 'prometheus_alerts') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
