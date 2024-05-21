{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "ping_instance_metric_id",
    on_schema_change="sync_all_columns"
) }}

{{ simple_cte([
    ('dim_ping_instance', 'dim_ping_instance'),
    ('dim_ping_metric', 'dim_ping_metric')
    ])

}}

, fct_ping_instance_metric AS (

    SELECT
      {{ dbt_utils.star(from=ref('fct_ping_instance_metric'), except=['METRIC_VALUE', 'CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE',
          'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }},
      TRY_TO_NUMBER(metric_value::TEXT) AS metric_value
    FROM {{ ref('fct_ping_instance_metric') }}
    {% if is_incremental() %}
    WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{this}})
    {% endif %}

),

time_frame_7_day_metrics AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['dim_installation_id', 'metrics_path', 'ping_created_at_date_week']) }} AS ping_instance_metric_week_pk,
      fct_ping_instance_metric.*,
      dim_ping_metric.time_frame
    FROM fct_ping_instance_metric
    INNER JOIN dim_ping_metric
      ON fct_ping_instance_metric.metrics_path = dim_ping_metric.metrics_path
    INNER JOIN dim_ping_instance
      ON fct_ping_instance_metric.dim_ping_instance_id = dim_ping_instance.dim_ping_instance_id
    WHERE time_frame = ('7d')
      AND is_last_ping_of_week = TRUE
      AND has_timed_out = FALSE
      AND metric_value IS NOT NULL

)

{{ dbt_audit(
    cte_ref="time_frame_7_day_metrics",
    created_by="@iweeks",
    updated_by="@mdrussell",
    created_date="2022-08-08",
    updated_date="2024-05-21"
) }}
