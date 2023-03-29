WITH date_spine AS (

  SELECT date_day FROM {{ ref('dim_date') }}
  WHERE date_day < GETDATE() AND date_day >= '2020-01-01'
),

plan_ids AS (

  SELECT DISTINCT
    gitlab_plan_id,
    CASE WHEN gitlab_plan_title IN ('Premium (Formerly Silver)', 'Ultimate (Formerly Gold)', 'Bronze') THEN 'Paid'
      WHEN gitlab_plan_title IN ('Free', 'Premium Trial', 'Ultimate Trial', 'Open Source Program') THEN 'Free'
      WHEN gitlab_plan_title IS NULL THEN 'Internal'
      ELSE 'Internal' END AS gitlab_plan_title
  FROM {{ ref('dim_namespace') }}

),

dim_namespace_plan_hist AS (

  SELECT * FROM {{ ref('dim_namespace_plan_hist') }}

)

SELECT
  date_spine.date_day,
  hist.dim_namespace_id,
  hist.dim_plan_id,
  COALESCE(plan_ids.gitlab_plan_title, 'internal') AS finance_pl
FROM date_spine
LEFT JOIN dim_namespace_plan_hist hist ON date_spine.date_day BETWEEN hist.valid_from AND COALESCE(hist.valid_to, GETDATE())
LEFT JOIN plan_ids ON plan_ids.gitlab_plan_id = hist.dim_plan_id
