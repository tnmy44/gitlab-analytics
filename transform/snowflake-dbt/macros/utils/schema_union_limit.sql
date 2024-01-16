{% macro schema_union_limit(schema_part, table_name, column_name, day_limit=30, database_name=none, boolean_filter_statement=none, local=none) %}

WITH base_union AS (

{%- if local != 'no' -%}

  {{ schema_union_all(schema_part, table_name, database_name=database_name, day_limit=day_limit, boolean_filter_statement=boolean_filter_statement, local='yes') }}

{%- else -%}

  {{ schema_union_all(schema_part, table_name, database_name=database_name, day_limit=day_limit, boolean_filter_statement=boolean_filter_statement, local=none) }}

{%- endif -%}


) 

SELECT *
FROM base_union
WHERE {{ column_name }} >= dateadd('day', -{{ day_limit }}, CURRENT_DATE())

{% endmacro %}
