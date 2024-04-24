WITH source AS (

  SELECT *
  FROM {{ ref('thanos_gitaly_rps') }}
  WHERE is_success = TRUE
)

SELECT
  DATE_TRUNC('day', metric_created_at)                                            AS day,
  AVG(metric_value)                                                               AS avg_gitaly_rps,
  SUM(metric_value * 3600)                                                        AS total_gitaly_requests,
  SUM(metric_value) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS rolling_7d_requests,
  SUM(metric_value) OVER (ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS rolling_28d_requests
FROM source
GROUP BY 1
