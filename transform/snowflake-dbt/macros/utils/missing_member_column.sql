{%- macro missing_member_column(primary_key, referential_integrity_columns=[], not_null_test_cols=[]) -%}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

    {%- set columns = adapter.get_columns_in_relation(this) -%}
    {%- set referential_integrity_columns = referential_integrity_cols|list -%}
    {%- set not_null_test_columns = not_null_test_cols|list -%}
    {%- set columns_for_insert = dbt_utils.get_filtered_columns_in_relation(this)|join(", ") -%}
    {%- set columns_for_insert_aliased =  "b." + dbt_utils.get_filtered_columns_in_relation(this)|join(", b.") -%}
    {%- set columns_from_insert_aliased =  "a." + dbt_utils.get_filtered_columns_in_relation(this)|join(", a.") -%}
    
    {%- set join_columns = [] %}
    {%- for col in columns %}
       {%-if not loop.last %}
         {%- do join_columns.append('a.{} = b.{} AND '.format(col.name, col.name)) %}
        {%-elif loop.last %}
         {%- do join_columns.append('a.{} = b.{}'.format(col.name, col.name)) %} 
      {%- endif %}
    {%- endfor %}

    {%- set update_columns = [] %}
    {%- for col in columns %}
      {%- do update_columns.append('a.{} = b.{}'.format(col.name, col.name)) %}
    {%- endfor %}

    {%- set join_columns_items = ''.join(join_columns) %}
    {%- set update_columns_items=  ', '.join(update_columns) %}


    MERGE INTO {{ this }} a USING (

    SELECT 
    {%- for col in columns %}
      {% if not loop.last %}
        {% if col.name|lower == primary_key %}
        '-1' AS {{ col.name|lower }},
        {% elif col.name|lower in referential_integrity_columns|lower %}
        '-1' AS {{ col.name|lower }},
        {% elif col.name|lower in not_null_test_columns|lower %}
        '0' AS {{ col.name|lower }},
        {% elif '_ID' in col.name|string %}
        '-1' AS {{ col.name|lower }},
        {% elif col.data_type == 'BOOLEAN' %}
        NULL AS {{ col.name|lower }},
        {% elif col.data_type == 'VARCHAR' %}
        'Unknown' AS {{ col.name|lower }},
        {% elif col.data_type == 'TEXT' %}
        'Unknown' AS {{ col.name|lower }},
        {% elif col.data_type == 'CHAR' %}
        'Unknown' AS {{ col.name|lower }},
        {% elif col.data_type == 'CHARACTER' %}
        'Unknown' AS {{ col.name|lower }},
        {% elif col.data_type.startswith('character varying') %}
        'Unknown' AS {{ col.name|lower }},
        {% elif col.data_type == 'STRING' %}
        'Unknown' AS {{ col.name|lower }},
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
        {% elif col.data_type == 'TIMESTAMP_LTZ' %}
        '9999-01-01 00:00:00.000 +0000' AS {{ col.name|lower }},
        {% elif col.data_type == 'FLOAT' %}
        NULL AS {{ col.name|lower }},
        {% elif col.data_type == 'NUMBER' %}
        NULL AS {{ col.name|lower }},
        {% elif col.data_type.startswith('NUMBER') %}
        NULL AS {{ col.name|lower }},
        {% elif col.data_type == 'NUMERIC' %}
        NULL AS {{ col.name|lower }},
        {% elif col.data_type == 'DECIMAL' %}
        NULL AS {{ col.name|lower }},
        {% elif col.data_type == 'INT' %}
        NULL AS {{ col.name|lower }},
        {% elif col.data_type == 'INTEGER' %}
        NULL AS {{ col.name|lower }},
        {% elif col.data_type == 'TINYINT' %}
        NULL AS {{ col.name|lower }},
        {% elif col.data_type == 'BIGINT' %}
        NULL AS {{ col.name|lower }},
        {% else %}
        NULL AS {{ col.name|lower }},
        {% endif %}
      {% elif loop.last %}
        {% if col.name|lower == primary_key %}
        '-1' AS {{ col.name|lower }}
        {% elif col.name|lower in referential_integrity_columns|lower %}
        '-1' AS {{ col.name|lower }}
        {% elif col.name|lower in not_null_test_columns|lower %}
        '0' AS {{ col.name|lower }}
        {% elif '_ID' in col.name|string %}
        '-1' AS {{ col.name|lower }}
        {% elif col.data_type == 'BOOLEAN' %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type == 'VARCHAR' %}
        'Unknown' AS {{ col.name|lower }}
        {% elif col.data_type == 'TEXT' %}
        'Unknown' AS {{ col.name|lower }}
        {% elif col.data_type == 'CHAR' %}
        'Unknown' AS {{ col.name|lower }}
        {% elif col.data_type == 'CHARACTER' %}
        'Unknown' AS {{ col.name|lower }}
        {% elif col.data_type.startswith('character varying') %}
        'Unknown' AS {{ col.name|lower }}
        {% elif col.data_type == 'STRING' %}
        'Unknown' AS {{ col.name|lower }}
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
        {% elif col.data_type == 'TIMESTAMP_LTZ' %}
        '9999-01-01 00:00:00.000 +0000' AS {{ col.name|lower }}
        {% elif col.data_type == 'FLOAT' %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type == 'NUMBER' %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type.startswith('NUMBER') %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type == 'NUMERIC' %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type == 'DECIMAL' %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type == 'INT' %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type == 'INTEGER' %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type == 'TINYINT' %}
        NULL AS {{ col.name|lower }}
        {% elif col.data_type == 'BIGINT' %}
        NULL AS {{ col.name|lower }}
        {% else %}
        NULL AS {{ col.name|lower }}
        {% endif %}
      {% endif %}
    {%- endfor %}

    FROM {{this}}
    LIMIT 1
    
    ) AS b ON {{join_columns_items}}
  WHEN MATCHED THEN UPDATE SET {{update_columns_items}} 
  WHEN NOT MATCHED THEN INSERT ({{ columns_for_insert }}) VALUES ({{ columns_for_insert_aliased }})


{%- endmacro -%}

