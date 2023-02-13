{%- macro samples() -%}

{#
Example
samples:
  - name: date_details_source
    clause: '{{ sample_table(3) }}'
  - name: date_details
    clause: "WHERE date_actual >= DATEADD('day', -30, CURRENT_DATE())"

#}

{% set samples_yml -%}

samples:
  - name: prep_ping_instance
    clause: '{{ sample_table(3) }}'
  - name: dim_date
    clause: '{{ sample_table(3) }}'

{%- endset %}

{% set samples_dict = fromyaml(samples_yml) %}

{% do return(samples_dict) %}


{%- endmacro -%}
