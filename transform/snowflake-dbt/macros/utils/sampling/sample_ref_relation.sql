{%- macro sample_ref_relation(model_name) -%}

  {% set relation = builtins.ref(model_name) %}
  {% set samples_dict = samples() %}
  {% set model_sample = samples_dict.samples | selectattr("name", "equalto",model_name) | list %} 

  {% set is_sampled = model_sample|length >0 %}

  {% set sample_override = var('local_data') == 'sample' %}
  {% set is_development = target.name not in production_targets() %}
  
  {% if is_development and is_sampled and sample_override %}
    {% set new_relation = relation.replace_path(identifier =relation.identifier ~ var('sample_suffix')) %}
    {% do return(new_relation) %}    
  {% else %}
    {% do return(relation) %}
  {% endif %}

  

{%- endmacro -%}
