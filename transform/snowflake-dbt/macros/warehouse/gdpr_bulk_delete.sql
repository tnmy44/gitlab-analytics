{% macro gdpr_bulk_delete() %}

{%- call statement('gdpr_deletions', fetch_result=True) %}
    select SHA2(TRIM(LOWER(email_address))), delete_type
    from {{ source('driveload', 'gdpr_delete_requests') }}
{%- endcall -%}

    {%- set value_list = load_result('gdpr_deletions') -%}

    {%- if value_list and value_list['data'] -%}

      {%- set values = value_list['data'] %}

      {% for data_row in values %}
        {% if data_row[1] == 'full' %}
        {{ gdpr_delete_test(data_row[0], run_queries='True')}}
        {% endif %}
        {% if data_row[1] == 'gitlab' %}
        {{ gdpr_gitlab_delete_test(data_row[0], run_queries='True')}}
        {% endif %}




      {% endfor %}
    {%- endif -%}

{%- endmacro -%}
