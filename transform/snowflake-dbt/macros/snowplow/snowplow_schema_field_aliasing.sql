{% macro snowplow_schema_field_aliasing(schema, context_name, field_alias_datatype_list = []) %}
  --{{context_name}} columns
  IFF(context_data_schema LIKE '{{schema}}', context_data, NULL)                              AS {{context_name}}_context,
  IFF(context_data_schema LIKE '{{schema}}', context_data_schema, NULL)                       AS {{context_name}}_context_schema,
{%- for context_col in field_alias_datatype_list %}
  {% set field = context_col[0] %}
  {% set formula = context_col[1] %}
  {% set data_type = context_col[2] %}
  {% set alias = context_col[3] %}

  {%- if formula.strip() == '' -%}
    {% set formula = "context_data['"+field+"']" %}
  {%- else -%}
    {% set formula = formula %}
  {%- endif -%}

  {%- if data_type.strip() == '' -%}
    {% set data_type = "VARCHAR" %}
  {%- else -%}
    {% set data_type = data_type|upper %}
  {%- endif -%}

  {%- if alias.strip() == '' -%}
    {% set alias = field %}
  {%- else -%}
    {% set alias = alias %}
  {%- endif -%}

  IFF(context_data_schema LIKE '{{schema}}', {{formula}}::{{data_type}}, NULL)  AS {{alias}}{%- if not loop.last %},{% endif %}
{%- endfor -%}
{% endmacro %}