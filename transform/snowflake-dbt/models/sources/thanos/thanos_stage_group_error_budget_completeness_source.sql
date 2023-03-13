WITH source AS (

    SELECT *
    FROM {{ source('thanos', 'periodic_queries') }}

), parsed AS (

    SELECT
        parse_json(pq_2.value:value[0]::DOUBLE)::int::timestamp_ntz as metric_created_at,
        NULLIF(pq_2.value:value[1],'NaN')::DOUBLE as metric_value,
        pq_1.value['data']['resultType']::VARCHAR as result_type,
        pq_1.value['status']:: VARCHAR AS status_type,
        pq_1.this['message']:: VARCHAR as message_type,
        pq_1.this['status_code']:: VARCHAR as status_code,
        pq_1.this['success']:: VARCHAR as is_success
    FROM
        source pq ,
        , lateral flatten(input => pq.jsontext['stage_group_error_budget_completeness']) pq_1
        , lateral flatten(input => pq.jsontext['stage_group_error_budget_completeness']['body']['data']['result'],outer => true) pq_2
    WHERE result_type IS NOT NULL AND status_type IS NOT NULL

)
    SELECT * FROM
    parsed