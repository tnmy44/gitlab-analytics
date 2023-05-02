WITH source AS (

    SELECT *
    FROM {{ ref('thanos_total_haproxy_bytes_out')}}

)

SELECT date_trunc('day', metric_created_at) as date_day,
metric_backend as haproxy_backend,
sum(metric_value)/pow(1024, 3) as usage_in_gb,
RATIO_TO_REPORT(SUM(usage_in_gb)) OVER (PARTITION BY date_day) AS percent_backend_ratio
FROM source
GROUP BY 1, 2
