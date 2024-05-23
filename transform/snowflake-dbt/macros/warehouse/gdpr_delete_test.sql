{% macro gdpr_delete_test(email_sha, run_queries=False) %}
        {% set set_sql %}
        SET email_sha = '{{email_sha}}';
        {% endset %}
        {{ log(set_sql, info = True) }}

{%- endmacro -%}
