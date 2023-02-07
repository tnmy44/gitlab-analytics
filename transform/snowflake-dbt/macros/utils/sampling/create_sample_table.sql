{%- macro create_sample_table(model_name) -%}

{%- set relation = builtins.ref(model_name) -%}
{%- set sample_relation = relation.replace_path(identifier =relation.identifier ~ var('sample_suffix')) -%}
{%- set samples_dict = samples() -%}
{%- set sample_clause = samples_dict.samples | selectattr("name", "equalto",model_name) | map(attribute='clause')| list -%}

CREATE OR REPLACE TRANSIENT TABLE {{ sample_relation }} AS SELECT * FROM {{ relation }} {{ sample_clause[0] }};

{%- endmacro -%}
