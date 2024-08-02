WITH date_spine AS (

  SELECT date_day FROM {{ ref('dim_date') }}
  WHERE date_day < GETDATE() AND date_day >= '2020-01-01'
),

plan_ids AS (

  SELECT DISTINCT
    gitlab_plan_id,
    FIRST_VALUE(CASE
      WHEN gitlab_plan_title = 'Default' THEN 'internal'
      WHEN gitlab_plan_is_paid = TRUE THEN 'paid'
      WHEN gitlab_plan_is_paid = FALSE THEN 'free'
      ELSE 'internal'
    END) OVER (PARTITION BY gitlab_plan_id ORDER BY IFF(gitlab_plan_is_paid = TRUE, 1, 0) DESC) AS gitlab_plan_title
  FROM {{ ref('dim_namespace') }}

),

dim_namespace_plan_hist AS (

  SELECT
    hist.*,
    ns.namespace_is_internal -- add namespace_is_internal to internal identification
  FROM {{ ref('dim_namespace_plan_hist') }} AS hist
  LEFT JOIN {{ ref('dim_namespace') }} AS ns ON hist.dim_namespace_id = ns.dim_namespace_id

),

final AS (

  SELECT
    date_spine.date_day,
    hist.dim_namespace_id,
    hist.dim_plan_id,
    CASE WHEN hist.namespace_is_internal THEN 'internal'
      WHEN hist.is_trial = TRUE THEN 'free' ELSE plan_ids.gitlab_plan_title
    END AS finance_pl
  FROM date_spine
  LEFT JOIN dim_namespace_plan_hist AS hist ON date_spine.date_day >= hist.valid_from AND date_spine.date_day < COALESCE(hist.valid_to, GETDATE())
  LEFT JOIN plan_ids ON hist.dim_plan_id = plan_ids.gitlab_plan_id

)

SELECT *
FROM final
