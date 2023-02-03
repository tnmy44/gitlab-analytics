{%- macro create_sample_table(model_name) -%}

{%- set relation = builtins.ref(model_name) -%}
{%- set sample_relation = relation.replace_path(identifier =relation.identifier ~ var('sample_suffix')) -%}
{%- set samples_dict = samples() -%}
{%- set sample_where = samples_dict.samples | selectattr("name", "equalto",model_name) | map(attribute='where')| list -%}

CREATE OR REPLACE TRANSIENT TABLE {{ sample_relation }} AS SELECT * FROM {{ relation }} WHERE {{ sample_where[0] }};

{%- endmacro -%}
