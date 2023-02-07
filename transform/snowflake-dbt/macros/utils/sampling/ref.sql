{% macro ref(model_name) %}

  {% set rel = builtins.ref(model_name) %}
  {% set sample_rel = sample_ref_relation(model_name) %}
  {% set newrel = sample_rel %}
  {% do return(newrel) %}

{% endmacro %}