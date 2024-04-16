WITH credits AS ( --excluding migration credit between march 7th and March 27th

  SELECT
    cr.source_primary_key,
    SUM(cr.credit_amount) AS total_credit
  FROM {{ ref('gcp_billing_export_credits') }} AS cr
  WHERE LOWER(cr.credit_description) NOT LIKE '%1709765302259%'
  GROUP BY cr.source_primary_key

)

,

daily_costs AS (

  SELECT *
  FROM {{ ref('gcp_billing_export_xf') }} AS xf
  WHERE DATE_TRUNC('month', DATE(usage_start_time)) >= '2023-11-01'
    AND xf.project_id IN ('unreview-poc-390200e5')
    AND xf.service_description = 'Vertex AI'

)

SELECT
  DATE(usage_start_time)                                       AS date_day,
  SUM(cost_before_credits) + COALESCE(SUM(cr.total_credit), 0) AS cost_before_credits
FROM daily_costs
LEFT JOIN credits AS cr ON daily_costs.source_primary_key = cr.source_primary_key
GROUP BY 1
