with sheetload as (

    select *
    from {{ ref('sheetload_pagerslack_stats_source')}}

)

select attempts,
weekend_pinged,
unavailable,
incident_url,
reported_by,
reported_at,
time_to_response,
time_at_response,
1                               AS escalations,
        CASE
            WHEN date_part('hour', time_at_response::TIMESTAMP) > 0
                AND date_part('hour', time_at_response::TIMESTAMP) < 8
            THEN 'APAC'
            WHEN date_part('hour', time_at_response::TIMESTAMP) >= 8
                AND date_part('hour', time_at_response::TIMESTAMP) <= 16
            THEN 'EMEA'
            ELSE 'AMER'
        END                             AS timezone,
        time_to_response::FLOAT/60000   AS minutes_to_response,
        iff(unavailable::BOOLEAN=True, 'Response', 'No response')
                                        AS response_type_copy,
        iff(unavailable::BOOLEAN=True, 'Escalated', 'Bot')
                                        AS response_type
    FROM
        sheetload