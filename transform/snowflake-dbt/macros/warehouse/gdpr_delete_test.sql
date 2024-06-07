{% macro gdpr_delete_test(email_sha, run_queries=False) %}


        {% set data_types = ('BOOLEAN', 'TIMESTAMP_TZ', 'TIMESTAMP_NTZ', 'FLOAT', 'DATE', 'NUMBER') %}
        {% set exclude_columns = ('dbt_scd_id', 'dbt_updated_at', 'dbt_valid_from', 'dbt_valid_to', '_task_instance', '_uploaded_at', '_sdc_batched_at', '_sdc_extracted_at',
           '_sdc_received_at', '_sdc_sequence', '_sdc_table_version') %}

        {% set set_sql %}
        SET email_sha = '{{email_sha}}';
        {% endset %}
        {{ log('{email_sha: ' ~ email_sha ~ '}', info = True) }}

         {% if set_sql %}
             {% set results = run_query(set_sql) %}
             {{ log("Snowflake response: " ~ results, info=True) }}
         {% endif %}


{%- endmacro -%}
