{%- macro missing_member_column(primary_key) -%}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

    {%- set columns = adapter.get_columns_in_relation(this) -%}

    CREATE OR REPLACE TABLE {{ this }} AS 
    
    SELECT 
      *,
      0 AS is_missing_member_record
    FROM {{ this }}

    UNION ALL 

    SELECT DISTINCT
    {%- for col in columns %}
      {% if col.name|lower == '{{ primary_key }}' %}
      '-1' AS {{ col.name|lower }},
      {% elif col.data_type == 'BOOLEAN' %}
      NULL AS {{ col.name|lower }},
      {% elif col.data_type == 'VARCHAR' %}
      NULL AS {{ col.name|lower }},
      {% elif col.data_type == 'TEXT' %}
      NULL AS {{ col.name|lower }},
      {% elif col.data_type == 'DATE' %}
      '9999-01-01' AS {{ col.name|lower }},
      {% elif col.data_type == 'DATETIME' %}
      '9999-01-01 00:00:00.000 +0000' AS {{ col.name|lower }},
      {% elif col.data_type == 'TIMESTAMP_TZ' %}
      '9999-01-01 00:00:00.000 +0000' AS {{ col.name|lower }},
      {% elif col.data_type == 'TIMESTAMP_NTZ' %}
      '9999-01-01 00:00:00.000 +0000' AS {{ col.name|lower }},
      {% elif col.data_type == 'TIMESTAMP' %}
      '9999-01-01 00:00:00 +0000' AS {{ col.name|lower }},
      {% elif col.data_type == 'FLOAT' %}
      NULL AS {{ col.name|lower }},
      {% elif col.data_type == 'NUMBER' %}
      NULL AS {{ col.name|lower }},
      {% elif col.data_type == 'NUMERIC' %}
      NULL AS {{ col.name|lower }},
      {% elif col.data_type == 'DECIMAL' %}
      NULL AS {{ col.name|lower }},
      {% elif col.data_type == 'INT' %}
      NULL AS {{ col.name|lower }},
      {% elif col.data_type == 'TINYINT' %}
      NULL AS {{ col.name|lower }},
      {% elif col.data_type == 'BIGINT' %}
      NULL AS {{ col.name|lower }},
      {% else %}
      NULL AS {{ col.name|lower }},
      {% endif %}
    {%- endfor %}
    1 AS is_missing_member_record

    FROM {{ this }}

{%- endmacro -%}

