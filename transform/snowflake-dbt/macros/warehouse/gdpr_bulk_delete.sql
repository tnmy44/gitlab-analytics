{% macro gdpr_bulk_delete(run_queries=False) %}

{%- call statement('gdpr_deletions', fetch_result=True) %}
    select SHA2(TRIM(LOWER(email_address)))
    from {{ source('driveload', 'gdpr_delete_requests') }}
    limit 10
{%- endcall -%}

    {%- set value_list = load_result('gdpr_deletions') -%}

    {%- if value_list and value_list['data'] -%}

      {%- set values = value_list['data'] %}

      {% for data_row in values %}

        {{ gdpr_delete_test(data_row[0])}}

      {% endfor %}
    {%- endif -%}

{%- endmacro -%}
