{{ config(materialized='table') }}

{{ rpt_ratio_sales_management_cut_generator(["sales_segment_grouped", "sales_qualified_source"], 'TRUE') }}
