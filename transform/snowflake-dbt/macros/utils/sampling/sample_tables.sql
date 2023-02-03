{%- macro sample_tables() -%}

  {% set samples_dict = samples() %}
  {% set sample_models = samples_dict.samples | selectattr("method", "equalto","table") |  map(attribute='name')| list %}

{% set sql %}

  {% for model in sample_models %}

    {{ create_sample_table(model) }}
    
  {% endfor %}

{% endset %}

{% do return(sql) %}
{#% do run_query(sql) %#}
{% do log("Tables Samples", info=True) %}

{%- endmacro -%}