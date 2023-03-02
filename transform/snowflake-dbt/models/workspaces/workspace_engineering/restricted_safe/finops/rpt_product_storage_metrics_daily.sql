
{{ config(
    materialized='table',
    )
}}

  -- dumb model for initial push
  SELECT * FROM {{ ref('gitlab_dotcom_projects_xf') }}
