{%- macro samples() -%}

{% set samples_yml -%}

samples:
  - name: dim_date
    method: table
    where: "date_actual >= DATEADD('day', -30, CURRENT_DATE())"
  - name: date_details_source
    method: random
    percent: 3
  - name: date_details
    method: table
    where: "date_actual >= DATEADD('day', -30, CURRENT_DATE())"

{%- endset %}

{% set samples_dict = fromyaml(samples_yml) %}

{% do return(samples_dict) %}


{%- endmacro -%}
