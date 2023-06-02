{%- macro missing_member_column(primary_key, referential_integrity_columns=[], not_null_test_cols=[]) -%}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

    {%- set columns = adapter.get_columns_in_relation(this) -%}
    {%- set referential_integrity_columns = referential_integrity_cols|list -%}
    {%- set not_null_test_columns = not_null_test_cols|list -%}

    -- create list of join conditions
    {%- set join_columns = [] %}
    {% for col in columns %}
      {% set join_condition %}
         a.{{col.name}} = b.{{col.name}}
      {% endset %}
      {% do join_columns.append(join_condition) %}
    {% endfor %}

    -- create list of columns for joins to update missing member column if the values are ever updated in this macro
    {%- set update_columns = [] %}
    {% for col in columns %}
      {% set update_condition = "a." + col.name + "= b." + col.name %}
      {% do update_columns.append(update_condition) %}
    {% endfor %}
    {%- set update_columns = update_columns | join(', ') -%}

    -- create a list of columns for inserting the missing member if it does not exist in the model
    {% set insert_columns = [] %}
    {% for col in columns -%}
        {%- do insert_columns.append(adapter.quote(col.name)) -%}
    {%- endfor %}
    {%- set insert_columns = insert_columns | join(', ') -%}


    -- logic to generate the default values for the missing member
    MERGE INTO {{ this }} a USING (

    SELECT 
    {%- for col in columns %}
      {% if col.name|lower == primary_key %}
      MD5('-1') AS {{ col.name|lower }}
      -- if the column is part of a relationship test, set it to -1 so this test does not fail
      {% elif col.name|lower in referential_integrity_columns|lower %}
      MD5('-1') AS {{ col.name|lower }}
      -- if the column is part of a not null test, set it to 0 so this test does not fail
      {% elif col.name|lower in not_null_test_columns|lower %}
      '0' AS {{ col.name|lower }}
      -- if the column contains "_ID" this indicates it is an id so assign it "-1"
      {% elif ('_ID' in col.name|string and col.data_type.startswith('character varying')) %}
      MD5('-1') AS {{ col.name|lower }}
      {% elif col.name|string == 'IS_DELETED' %}
      '0' AS {{ col.name|lower }}
      -- boolean
      {% elif col.data_type == 'BOOLEAN' %}
      NULL AS {{ col.name|lower }}
      -- strings
      {% elif (col.data_type.startswith('character varying') and col.string_size() >= ('Missing '+ col.name)|length) %}
      'Missing ' || '{{ col.name|lower }}' AS {{ col.name|lower }}
      {% elif (col.data_type.startswith('character varying') and col.string_size() < ('Missing '+ col.name)|length) %}
      'Missing' AS {{ col.name|lower }}
      -- dates
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
      -- numeric
      {% elif col.data_type.startswith('NUMBER') %}
      NULL AS {{ col.name|lower }}
      -- catch all for everything else
      {% else %}
      NULL AS {{ col.name|lower }}
      {%- endif %}
      {%- if not loop.last %}
      , 
      {%- endif %}
    {%- endfor %}
    
    ) AS b 
  ON {{"(" ~ join_columns| join(") and (") ~ ")"}}
  WHEN MATCHED THEN UPDATE SET 
    {{ update_columns }}
  WHEN NOT MATCHED THEN INSERT
  ({{ insert_columns }})
  VALUES
  ({{ insert_columns }})


{%- endmacro -%}

