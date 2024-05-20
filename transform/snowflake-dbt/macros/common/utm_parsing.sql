{%- macro utm_parsing(utm_column_name) -%}

CASE 
    WHEN {{utm_column_name}} = 'utm_campaign' 
        THEN 
            REGEXP_COUNT({{utm_column_name}}, '_') >= 6 AS uses_new_utm_format,
            CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 1) END AS {{utm_column_name}}_date,
            CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 2) END AS {{utm_column_name}}_region,
            CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 3) END AS {{utm_column_name}}_budget,
            CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 4) END AS {{utm_column_name}}_type,
            CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 5) END AS {{utm_column_name}}_gtm,
            CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 6) END AS {{utm_column_name}}_language,
            CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 7) END AS {{utm_column_name}}_agency,
            CASE WHEN uses_new_utm_format THEN 
                RIGHT({{utm_column_name}} , LEN({{utm_column_name}} ) - regexp_instr({{utm_column_name}} ,'_',1,8))
                END AS {{utm_column_name}}_name,
    WHEN {{utm_column_name}} = 'utm_content'
        THEN 
            REGEXP_COUNT({{utm_column_name}}, '_') >= 2 AS uses_new_utm_format,
            CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 1) END AS utm_content_offer,
            CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 2) END AS utm_content_asset_type,
            CASE WHEN uses_new_utm_format THEN SPLIT_PART({{utm_column_name}} , '_', 3) END AS utm_content_industry,
        END
{%- endmacro -%}
