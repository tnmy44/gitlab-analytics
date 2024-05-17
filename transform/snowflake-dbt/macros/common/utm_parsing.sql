{%- macro utm_parsing(utm_column_name) -%}

REGEXP_COUNT({{utm_column_name}}, '_') >= 6 AS uses_new_utm_format,
CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 1) END AS {{utm_column_name}}_date,
        CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 2) END AS {{utm_column_name}}_region,
        CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 3) END AS {{utm_column_name}}_budget,
        CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 4) END AS {{utm_column_name}}_type,
        CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 5) END AS {{utm_column_name}}_gtm,
        CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 6) END AS {{utm_column_name}}_language,
        CASE WHEN uses_new_utm_format THEN 
            RIGHT({{utm_column_name}} , LEN({{utm_column_name}} ) - regexp_instr({{utm_column_name}} ,'_',1,7))
        END AS {{utm_column_name}}_name,

{%- endmacro -%}
