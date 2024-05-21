{%- macro utm_parsing(utm_column_name) -%}

CASE WHEN {{utm_column_name}} = 'utm_campaign' AND REGEXP_COUNT({{utm_column_name}}, '_') >= 6  THEN SPLIT_PART({{utm_column_name}} , '_', 1) END AS {{utm_column_name}}_date,
CASE WHEN {{utm_column_name}} = 'utm_campaign' AND REGEXP_COUNT({{utm_column_name}}, '_') >= 6  THEN SPLIT_PART({{utm_column_name}} , '_', 2) END AS {{utm_column_name}}_region,
CASE WHEN {{utm_column_name}} = 'utm_campaign' AND REGEXP_COUNT({{utm_column_name}}, '_') >= 6  THEN SPLIT_PART({{utm_column_name}} , '_', 3) END AS {{utm_column_name}}_budget,
CASE WHEN {{utm_column_name}} = 'utm_campaign' AND REGEXP_COUNT({{utm_column_name}}, '_') >= 6  THEN SPLIT_PART({{utm_column_name}} , '_', 4) END AS {{utm_column_name}}_type,
CASE WHEN {{utm_column_name}} = 'utm_campaign' AND REGEXP_COUNT({{utm_column_name}}, '_') >= 6  THEN SPLIT_PART({{utm_column_name}} , '_', 5) END AS {{utm_column_name}}_gtm,
CASE WHEN {{utm_column_name}} = 'utm_campaign' AND REGEXP_COUNT({{utm_column_name}}, '_') >= 6  THEN SPLIT_PART({{utm_column_name}} , '_', 6) END AS {{utm_column_name}}_language,
CASE WHEN {{utm_column_name}} = 'utm_campaign' AND REGEXP_COUNT({{utm_column_name}}, '_') >= 6  THEN SPLIT_PART({{utm_column_name}} , '_', 7) END AS {{utm_column_name}}_agency,
CASE WHEN {{utm_column_name}} = 'utm_campaign' AND REGEXP_COUNT({{utm_column_name}}, '_') >= 6  THEN 
                RIGHT({{utm_column_name}} , LEN({{utm_column_name}} ) - regexp_instr({{utm_column_name}} ,'_',1,8))
                END AS {{utm_column_name}}_name,
CASE WHEN {{utm_column_name}} = 'utm_content' AND REGEXP_COUNT({{utm_column_name}}, '_') >= 2  THEN SPLIT_PART({{utm_column_name}} , '_', 1) END AS utm_content_offer,
CASE WHEN {{utm_column_name}} = 'utm_content' AND REGEXP_COUNT({{utm_column_name}}, '_') >= 2  THEN SPLIT_PART({{utm_column_name}} , '_', 2) END AS utm_content_asset_type,
CASE WHEN {{utm_column_name}} = 'utm_content' AND REGEXP_COUNT({{utm_column_name}}, '_') >= 2  THEN SPLIT_PART({{utm_column_name}} , '_', 3) END AS utm_content_industry,
{%- endmacro -%}
