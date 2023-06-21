{%- macro samples() -%}

{#
Example
  samples:
    - name: date_details_source
      clause: "{{ sample_table(3) }}"
    - name: dim_date
      clause: "WHERE date_actual >= DATEADD('day', -30, CURRENT_DATE())"

#}

  {% set samples_yml -%}

  samples:
    - name: version_raw_usage_data_source
      clause: "{{ sample_table(1) }}"
    - name: instance_combined_metrics
      clause: "{{ sample_table(1) }}"
    - name: version_usage_data_source
      clause: "{{ sample_table(1) }}"


  {%- endset %}

  {% set samples_dict = fromyaml(samples_yml) %}

  {% do return(samples_dict) %}


{%- endmacro -%}
