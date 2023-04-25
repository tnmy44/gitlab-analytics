WITH source AS (

SELECT * FROM {{ source('thanos', 'periodic_queries') }}
WHERE source_file_name like '%daily-slas%'

), overall_ratio AS (

    SELECT 
        'overall_ratio'                                                 AS metric_query,
        pq_2.value['metric']['env']::VARCHAR                            AS metric_env,
        pq_2.value['metric']['environment']::VARCHAR                    AS metric_environment,
        pq_2.value['metric']['monitor']::VARCHAR                        AS metric_monitor,
        pq_2.value['metric']['ruler_cluster']::VARCHAR                  AS metric_ruler_cluster,
        pq_2.value['metric']['sla_type']::VARCHAR                       AS metric_sla_type,
        pq_2.value['metric']['stage']::VARCHAR                          AS metric_stage,
        parse_json(pq_2.value:value[0]::FLOAT)::NUMBER::TIMESTAMP       AS metric_created_at,
        NULLIF(pq_2.value:value[1],'NaN')::FLOAT                        AS metric_value,
        pq_1.value['data']['resultType']::VARCHAR                       AS result_type,
        pq_1.value['status']:: VARCHAR                                  AS status_type,
        pq_1.this['message']:: VARCHAR                                  AS message_type,
        pq_1.this['status_code']:: NUMBER                               AS status_code,
        pq_1.this['success']:: BOOLEAN                                  AS is_success
    FROM
        source pq,
        lateral flatten(input => pq.jsontext, outer => true) pq_base,
        lateral flatten(input => pq.jsontext['overall_ratio']) pq_1,
        lateral flatten(input => pq.jsontext['overall_ratio']['body']['data']['result'],outer => true) pq_2
    WHERE result_type IS NOT NULL AND status_type IS NOT NULL
)

, overall_target AS (

    SELECT 
        'overall_target'                                                AS metric_query,
        NULL                                                            AS metric_env,
        NULL                                                            AS metric_environment,
        pq_2.value['metric']['monitor']::VARCHAR                        AS metric_monitor,
        pq_2.value['metric']['ruler_cluster']::VARCHAR                  AS metric_ruler_cluster,
        NULL                                                            AS metric_sla_type,
        NULL                                                            AS metric_stage,
        parse_json(pq_2.value:value[0]::FLOAT)::NUMBER::TIMESTAMP       AS metric_created_at,
        NULLIF(pq_2.value:value[1],'NaN')::FLOAT                        AS metric_value,
        pq_1.value['data']['resultType']::VARCHAR                       AS result_type,
        pq_1.value['status']:: VARCHAR                                  AS status_type,
        pq_1.this['message']:: VARCHAR                                  AS message_type,
        pq_1.this['status_code']:: NUMBER                               AS status_code,
        pq_1.this['success']:: BOOLEAN                                  AS is_success
    FROM
        source pq,
        lateral flatten(input => pq.jsontext['overall_target']) pq_1,
        lateral flatten(input => pq.jsontext['overall_target']['body']['data']['result'],outer => true) pq_2
    WHERE result_type IS NOT NULL AND status_type IS NOT NULL
)

SELECT * FROM overall_ratio
UNION ALL 
SELECT * FROM overall_target
