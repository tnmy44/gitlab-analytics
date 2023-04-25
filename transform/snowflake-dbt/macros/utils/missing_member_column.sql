{%- macro missing_member_column(primary_key, referential_integrity_columns=[], not_null_test_cols=[]) -%}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

    {%- set columns = adapter.get_columns_in_relation(this) -%}
    {%- set referential_integrity_columns = referential_integrity_cols|list -%}
    {%- set not_null_test_columns = not_null_test_cols|list -%}
    {%- set columns_for_insert = dbt_utils.get_filtered_columns_in_relation(this)|list -%}

    MERGE INTO {{ this }} USING (

    SELECT 
    {%- for col in columns %}
      {% if not loop.last %}
        {% if col.name|lower == primary_key %}
        '-1' AS {{ col.name|lower }},
        {% elif col.name in referential_integrity_columns %}
        '-1' AS {{ col.name|lower }},
        {% elif col.name in not_null_test_columns %}
        '0' AS {{ col.name|lower }},
        {% elif '_ID' in col.name|string %}
        '-1' AS {{ col.name|lower }},
        {% elif col.name|lower == 'is_deleted' %}
        '0' AS {{ col.name|lower }},
        {% elif col.data_type == 'BOOLEAN' %}
        NULL AS {{ col.name|lower }},
        {% elif col.data_type == 'VARCHAR' %}
        'not available' AS {{ col.name|lower }},
        {% elif col.data_type == 'TEXT' %}
        'not available' || '{{ col.name|lower }}' AS {{ col.name|lower }},
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
        'not available' AS {{ col.name|lower }},
        {% endif %}
      {% elif loop.last %}
        {% if col.name|lower == primary_key %}
        '-1' AS {{ col.name|lower }}
        {% elif col.name in referential_integrity_columns %}
        '-1' AS {{ col.name|lower }}
        {% elif col.name in not_null_test_columns %}
        '0' AS {{ col.name|lower }}
        {% elif '_ID' in col.name|string %}
        '-1' AS {{ col.name|lower }}
        {% elif col.name|lower == 'is_deleted' %}
        '0' AS {{ col.name|lower }}
        {% elif col.data_type == 'BOOLEAN' %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type == 'VARCHAR' %}
        'missing_' || '{{ col.name|lower }}' AS {{ col.name|lower }}
        {% elif col.data_type == 'TEXT' %}
        'missing_' || '{{ col.name|lower }}' AS {{ col.name|lower }}
        {% elif col.data_type == 'DATE' %}
        '9999-01-01' AS {{ col.name|lower }}
        {% elif col.data_type == 'DATETIME' %}
        '9999-01-01 00:00:00.000 +0000' AS {{ col.name|lower }}
        {% elif col.data_type == 'TIMESTAMP_TZ' %}
        '9999-01-01 00:00:00.000 +0000' AS {{ col.name|lower }}
        {% elif col.data_type == 'TIMESTAMP_NTZ' %}
        '9999-01-01 00:00:00.000 +0000' AS {{ col.name|lower }}
        {% elif col.data_type == 'TIMESTAMP' %}
        '9999-01-01 00:00:00 +0000' AS {{ col.name|lower }}
        {% elif col.data_type == 'FLOAT' %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type == 'NUMBER' %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type == 'NUMERIC' %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type == 'DECIMAL' %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type == 'INT' %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type == 'TINYINT' %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type == 'BIGINT' %}
        NULL AS {{ col.name|lower }}
        {% else %}
        'not available' AS {{ col.name|lower }}
        {% endif %}
      {% endif %}
    {%- endfor %}

    FROM {{this}}
    LIMIT 1
    
    ) AS b ON {{ this }}.dim_crm_account_id = b.dim_crm_account_id
  WHEN NOT MATCHED THEN INSERT ({{columns_for_insert}}) VALUES ({{dbt_utils.get_column_values}})


{%- endmacro -%}

